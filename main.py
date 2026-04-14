"""
River Sentinel — FastAPI Dashboard Backend
==========================================
This is the central brain of the entire River Sentinel system.
It is a web server (FastAPI) that:
  - Serves the HTML dashboard UI at http://localhost:8000/
  - Manages a 3-step remote sensing pipeline via REST API endpoints
  - Streams live pipeline logs to the browser in real-time (Server-Sent Events)
  - Launches and monitors a GCP cloud VM for heavy merge jobs
  - Runs the DEM (Digital Elevation Model) cleaning tool
  - Runs Terraform to provision/destroy cloud infrastructure

The 3-step pipeline:
  Step 1 (GEE Export)      — Submits satellite imagery export tasks to Google Earth Engine
  Step 2 (Drive Download)  — Downloads the completed GEE exports from Google Drive to local disk
  Step 3 (Merge Tiles)     — Merges the downloaded GeoTIFF tile fragments into one file per river

Run the server with:
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload

Why FastAPI?
  - Async-native: pipeline steps run as background tasks without blocking the server
  - Built-in JSON validation and HTTP error helpers
  - Easy to serve Server-Sent Events (SSE) for live log streaming
"""

# ── Standard library imports ───────────────────────────────────────────────────

from fastapi import FastAPI, Request, HTTPException
# FastAPI   — the web framework that handles HTTP routing
# Request   — gives access to request body, headers, connection state
# HTTPException — raises structured HTTP errors (4xx/5xx) with JSON bodies

from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse
# StreamingResponse — used for Server-Sent Events (live log stream)
# JSONResponse      — explicit JSON response (mostly not needed, FastAPI auto-converts dicts)
# HTMLResponse      — serves the raw index.html page

from contextlib import asynccontextmanager
# asynccontextmanager — turns an async generator into a context manager
# Used for the lifespan hook that runs code on startup/shutdown

import asyncio
# asyncio — Python's async/await runtime
# Used to: create background Tasks, await I/O, run blocking calls in thread pool

import json
# json — read/write config.json, serialize log payloads in SSE stream

import os
# os — environment variables, path existence checks, mkdir

import re
# re — regular expressions; used to strip ANSI color escape codes from subprocess output

import sys
# sys — detect OS platform (win32 vs linux) to find the correct Python binary in venv

import tempfile
# tempfile — create a temporary file on disk for the Terraform/VM startup script

import shutil
# shutil — `shutil.which("terraform")` finds the terraform binary in PATH

import time
# time — not directly used in main.py, but imported for completeness

import subprocess
# subprocess — spawn child processes (run_stepN.py, gcloud CLI, terraform CLI)
# subprocess.Popen is used for non-blocking process launch with pipe capture

import threading
# threading — read stdout/stderr of child processes in background threads
# (so the async event loop is not blocked while waiting for subprocess output)

from datetime import datetime
# datetime — timestamp generation for log entries and pipeline start time

from pathlib import Path
# Path — OS-agnostic file path handling (avoids manual string concatenation)

from typing import Optional
# Optional — type hint: Optional[X] means the variable can be X or None


# ── Strip ANSI escape codes ────────────────────────────────────────────────────
# Tools like Terraform and gcloud output coloured text using ANSI codes like \x1B[32m.
# The browser terminal doesn't render these — they appear as garbage characters.
# This regex matches the full ANSI escape sequence pattern (CSI sequences, etc.)
_ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

def _strip_ansi(text: str) -> str:
    """Remove ANSI terminal colour/format codes from a string.

    Called on every line of Terraform output before it is added to the log.
    Not called on runner output (gee_export, merge_tiles) because those scripts
    don't use ANSI codes.
    """
    return _ANSI_ESCAPE.sub('', text)


# =============================================================================
# PATHS
# =============================================================================
# All paths are derived from the location of this file (main.py), so the
# project can be moved anywhere on disk without breaking anything.

ROOT_DIR      = Path(__file__).parent        # Project root: .../river project aiq/
CONFIG_FILE   = ROOT_DIR / "config.json"     # User configuration (persisted between runs)
RUNNERS_DIR   = ROOT_DIR / "runners"         # Folder with run_step1/2/3.py
TEMPLATES_DIR = ROOT_DIR / "templates"       # Folder with index.html (the dashboard UI)


# =============================================================================
# STATE
# =============================================================================
# pipeline_state is the single source of truth about what the pipeline is doing.
# It is read by the frontend via /api/status (polled) and /api/logs/stream (SSE).
# It is a plain dict because it only needs to be shared within a single process —
# no databases or Redis needed.

pipeline_state: dict = {
    "status":          "idle",    # Current pipeline status:
                                  #   "idle"           — nothing running, ready to start
                                  #   "running_step1"  — GEE export tasks being submitted
                                  #   "done_step1"     — export submitted, waiting for GEE
                                  #   "running_step2"  — downloading files from Drive
                                  #   "done_step2"     — download complete
                                  #   "running_step3"  — merging GeoTIFF tiles locally
                                  #   "done"           — all 3 steps complete
                                  #   "error"          — a step failed (see error_msg)
    "current_step":    0,         # Which step number is active (0 = none, 1–3)
    "selected_rivers": [],        # River names chosen by the user in the UI (e.g. ["Ganga", "Yamuna"])
    "started_at":      None,      # ISO timestamp of when Step 1 was kicked off
    "error_msg":       None,      # Human-readable error message if status == "error"
}

# pipeline_logs holds every log line ever emitted during the current run.
# Format of each entry: {"ts": "HH:MM:SS", "msg": "some message"}
# The list is CLEARED when a new pipeline run starts (reset or step1).
# The SSE stream sends each entry once by tracking how many have been sent.
pipeline_logs: list[dict] = []

# log_event is an asyncio.Event used to wake up the SSE generator
# whenever a new log line is added. Without this, the SSE loop would
# need to busy-poll pipeline_logs — the Event makes it efficient.
log_event:     asyncio.Event = None          # Initialised in lifespan (after event loop starts)

# active_process holds a reference to the currently running subprocess.Popen.
# Needed so /api/pipeline/reset can kill it forcibly.
active_process: Optional[subprocess.Popen] = None

# active_task holds a reference to the currently running asyncio.Task.
# Needed so /api/pipeline/reset can cancel it.
active_task:    Optional[asyncio.Task]     = None


# =============================================================================
# CONFIG
# =============================================================================
# DEFAULT_CONFIG defines EVERY configuration key the system uses, with sensible
# fallback values. When config.json exists on disk, its values override these.
# When a key is missing from config.json (e.g. after an upgrade adds a new key),
# load_config() fills it in from here via dict.setdefault().

DEFAULT_CONFIG = {
    # ── Paths ──────────────────────────────────────────────────────────────────
    "shapefile_path":       "",               # Absolute path to the river shapefile (.shp)
                                              # e.g. "C:/Users/.../River_India_Final.shp"
                                              # If set, river names are read from the shapefile.
                                              # If empty, FALLBACK_RIVERS list is used instead.
    "output_base_folder":   "",               # Root folder where downloaded imagery is saved
                                              # e.g. "C:/Users/.../Imagery_Output"
    "sentinel_subfolder":   "Sentinel",       # Sub-folder inside output_base_folder for Sentinel-2 tiles
    "dem_subfolder":        "DEM",            # Sub-folder inside output_base_folder for DEM tiles

    # ── Google Drive Authentication ─────────────────────────────────────────────
    "credentials_file":     "",               # Path to client_secret.json (OAuth2 client credentials)
                                              # Download from Google Cloud Console → APIs → Credentials
    "token_file":           "",               # Path to drive_token.pickle (cached OAuth2 token)
                                              # Auto-created on first login; subsequent runs reuse it

    # ── Google Drive Folder Names ───────────────────────────────────────────────
    "drive_folder":         "River_Imagery_Batch",           # Root folder in Google Drive
    "drive_sentinel_folder": "River_Imagery_Batch/Sentinel", # Sentinel tiles folder in Drive
    "drive_dem_folder":     "River_Imagery_Batch/DEM",       # DEM tiles folder in Drive

    # ── Google Earth Engine ──────────────────────────────────────────────────────
    "gee_project":          "plucky-sight-423703-k5",        # GEE Cloud Project ID
                                                              # Must match the project where
                                                              # the Earth Engine API is enabled

    # ── Export Settings ──────────────────────────────────────────────────────────
    "export_target":        "drive",          # Where to export imagery:
                                              #   "drive" → Google Drive (for Step 2 download)
                                              #   "gcs"   → Google Cloud Storage
                                              #   "both"  → both destinations simultaneously
    "selected_bands":       ["B2", "B3", "B4", "B5", "B6", "B7", "B8", "B8A", "B11", "B12"],
                                              # Sentinel-2 spectral bands to export.
                                              # B2=Blue, B3=Green, B4=Red, B5-B8A=Red Edge/NIR,
                                              # B11/B12=SWIR. Used for vegetation/water analysis.

    # ── Google Cloud Storage ─────────────────────────────────────────────────────
    "gcs_bucket":           "aiq-river-imagery",  # GCS bucket name
    "gcs_sentinel_prefix":  "Sentinel",             # Prefix (folder) for raw Sentinel tiles in GCS
    "gcs_merged_prefix":    "Sentinel_Merged",      # Prefix (folder) for merged outputs in GCS

    # ── Processing Parameters ────────────────────────────────────────────────────
    "buffer_distance":      10000,  # River buffer in metres (10 km each side)
                                    # Defines the export bounding box around the river centerline
    "resolution":           10,     # Export pixel size in metres (10m = native Sentinel-2 resolution)
    "max_cloud_cover":      10,     # Maximum acceptable cloud cover % per scene
                                    # Scenes with more cloud than this are excluded from the median composite
    "max_concurrent_tasks": 100,    # GEE allows up to 3000 tasks but throttle to 100 for safety
    "skip_existing":        True,   # If True, skip rivers that already have local .tif files
    "start_date":           "2025-01-01",  # Start of date window for imagery search
    "end_date":             "2025-12-31",  # End of date window for imagery search

    # ── Runtime (not shown in UI) ─────────────────────────────────────────────────
    "selected_rivers":      [],             # Written here by /api/pipeline/step1 before spawning runner
                                            # run_step1.py reads this to know which rivers to process
}


def load_config() -> dict:
    """Load config.json from disk, filling in any missing keys from DEFAULT_CONFIG.

    Why merge with DEFAULT_CONFIG?
      New versions of the app may add new config keys. If the user's config.json
      was written by an older version, it won't have those keys yet. setdefault()
      adds them with sensible defaults without overwriting what the user already set.

    Returns:
        dict — a complete config dictionary with all keys present.
    """
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            cfg = json.load(f)              # Load user's saved config
        for k, v in DEFAULT_CONFIG.items():
            cfg.setdefault(k, v)            # Fill missing keys from defaults
        return cfg
    return dict(DEFAULT_CONFIG)             # No config file yet → return a copy of defaults


def save_config(cfg: dict):
    """Persist config dict to config.json with pretty-printing.

    Called by:
      - /api/config (POST) — when user saves settings from the UI
      - /api/pipeline/step1 — to save selected_rivers before the runner reads them

    Why indent=2?
      Makes the file human-readable so users can hand-edit it if needed.
    """
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)


# =============================================================================
# LOGGING
# =============================================================================

def add_log(message: str):
    """Append a timestamped log entry to pipeline_logs and wake up SSE clients.

    This is the central logging function. Every part of the backend calls
    add_log() to surface information to the browser's live log panel.

    Args:
        message: Plain text message (no ANSI codes — those are stripped upstream).

    Side effects:
        - Appends {"ts": "HH:MM:SS", "msg": message} to pipeline_logs
        - Calls log_event.set() to wake up all /api/logs/stream SSE generators
          so they immediately push the new log line to connected browsers.
          Without this, clients would only see new logs after the 1-second timeout.
    """
    ts = datetime.now().strftime("%H:%M:%S")  # e.g. "14:32:07"
    pipeline_logs.append({"ts": ts, "msg": message})
    if log_event:               # Guard: log_event is None before lifespan runs
        log_event.set()         # Signal all awaiting SSE generators to wake up


# =============================================================================
# RIVER LIST
# =============================================================================
# FALLBACK_RIVERS is a static list of ~120 Indian river names.
# It is used when:
#   a) No shapefile path is configured in config.json
#   b) The shapefile path is set but the file doesn't exist
#   c) The shapefile exists but geopandas fails to read it
# This ensures the UI always has a river list to display even without a shapefile.
# The set() deduplicates any accidental duplicates; sorted() ensures alphabetical order.

FALLBACK_RIVERS = sorted({
    "Ajay", "Alaknanda", "Ambika", "Arkavathi", "Baitarani", "Barak", "Beas",
    "Betwa", "Bhagirathi", "Bhadra", "Bharathapuzha", "Bhavani", "Bhima",
    "Brahmani", "Brahmaputra", "Cauvery", "Chalakudy", "Chaliyar",
    "Chambal", "Chenab", "Damodar", "Dibang", "Ganga", "Gandak",
    "Ghaggar", "Girna", "Godavari", "Gomati", "Ghaghara",
    "Hemavathi", "Indravati", "Indus", "Jhelum", "Kabini",
    "Kali", "Kanhan", "Kangsabati", "Kaveri", "Ken", "Koyna",
    "Kosi", "Krishna", "Lohit", "Luni", "Mahi", "Mahanadi",
    "Mahananda", "Mandakini", "Manjira", "Manas", "Mayurakshi",
    "Meenachil", "Moyar", "Musi", "Narmada", "Nagavali",
    "Noyyal", "Pampa", "Palar", "Pench", "Pennar",
    "Penganga", "Periyar", "Pindar", "Ponnaiyar", "Pranhita",
    "Purna", "Ramganga", "Rapti", "Ravi", "Rihand",
    "Rupnarayan", "Rushikulya", "Sabari", "Sabarmati", "Sankosh",
    "Saryu", "Sharda", "Shimsha", "Sileru", "Son",
    "Subarnarekha", "Subansiri", "Sutlej", "Tapti", "Tapi",
    "Tawa", "Teesta", "Tons", "Tungabhadra", "Vaigai",
    "Vamsadhara", "Vellar", "Vedavati", "Vishwamitri",
    "Wainganga", "Wardha", "Yamuna", "Ghataprabha", "Malaprabha",
    "Tunga", "Kumudvathi", "Hagari", "Cheyyar", "Vegavathy",
    "Chittar", "Neyyar", "Kodayar", "Kallar", "Ullhas",
    "Savitri", "Vashishti", "Dhadhar", "Watrak", "Rupen",
    "Banas", "Torsa", "Jaldhaka", "Raidak", "Piyain", "Gaula",
    "Bagmati", "Kamla", "Balan", "Burhi Gandak", "Kiul",
    "Punpun", "Falgu", "Sone", "Kanhar", "Johilla",
    "Denwa", "Shakkar", "Sher",
})


# =============================================================================
# APP LIFESPAN
# =============================================================================
# The @asynccontextmanager lifespan pattern replaces the old @app.on_event("startup").
# Code before `yield` runs ONCE when the server starts (after the event loop is ready).
# Code after `yield` (if any) runs ONCE when the server shuts down.
# Why do we need this? Because asyncio.Event() must be created INSIDE a running
# event loop — creating it at module-level would fail or attach to the wrong loop.

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan hook: runs startup code, then yields to run the app."""
    global log_event
    # Create the asyncio.Event now that the event loop is running.
    # All SSE generators will wait on this event; add_log() will set it.
    log_event = asyncio.Event()
    add_log("River Sentinel dashboard ready.")   # First log line (visible in live console)
    yield   # Hand control to FastAPI — the app runs until shutdown is requested


# Create the FastAPI application instance.
# title= appears in the auto-generated /docs (Swagger UI) page.
# lifespan= wires up our startup hook above.
app = FastAPI(title="River Sentinel Dashboard", lifespan=lifespan)


# =============================================================================
# ROUTES — Pages
# =============================================================================

@app.get("/")
async def dashboard():
    """Serve the single-page dashboard UI.

    Reads templates/index.html from disk on every request.
    Why not use Jinja2 templating?  The UI is self-contained pure HTML/JS/CSS —
    no server-side rendering is needed, so reading the file directly is simpler.

    Returns:
        HTMLResponse — the full HTML page with Content-Type: text/html
    """
    html_file = TEMPLATES_DIR / "index.html"
    return HTMLResponse(html_file.read_text(encoding="utf-8"))


# =============================================================================
# ROUTES — Rivers
# =============================================================================

@app.get("/api/rivers")
async def get_rivers():
    """Return the list of available river names for the selection UI.

    Priority order:
      1. Read from shapefile if shapefile_path is configured and the file exists.
         Uses geopandas to open the file and extract river name column values.
      2. Fall back to the static FALLBACK_RIVERS list if shapefile is unavailable.

    The "source" field in the response tells the frontend which path was taken,
    so it can display "Loaded from shapefile (N rivers)" or "Using built-in list".

    Data flow:
        config.json → shapefile_path → geopandas.read_file() → column values
        OR
        FALLBACK_RIVERS (hardcoded list)

    Returns:
        {"rivers": [...], "count": N, "source": "shapefile" | "fallback"}
    """
    cfg      = load_config()
    shapefile = cfg.get("shapefile_path", "")  # Could be "" if not set
    rivers   = []
    source   = "fallback"                       # Default assumption

    if shapefile and os.path.exists(shapefile):
        try:
            import geopandas as gpd             # Lazy import — not needed if no shapefile
            gdf = gpd.read_file(shapefile)
            # Try multiple possible column names for river names
            # (different shapefiles use different conventions)
            for col in ("rivname", "name", "NAME", "RIVNAME", "RiverName", "River_Name"):
                if col in gdf.columns:
                    # dropna() removes NaN rows, unique() deduplicates, tolist() converts to Python list
                    rivers = sorted(gdf[col].dropna().unique().tolist())
                    source = "shapefile"
                    break
        except Exception as e:
            add_log(f"WARN: Shapefile read error: {e}")  # Non-fatal — fall through to fallback

    if not rivers:                              # shapefile not available or had no name column
        rivers = list(FALLBACK_RIVERS)

    return {"rivers": rivers, "count": len(rivers), "source": source}


# =============================================================================
# ROUTES — Config
# =============================================================================

@app.get("/api/config")
async def get_config():
    """Return the current configuration as JSON.

    Called by the Settings panel in the UI to populate form fields.
    Always returns a complete config (missing keys filled from defaults).
    """
    return load_config()


@app.post("/api/config")
async def update_config(request: Request):
    """Merge posted JSON fields into config.json and save.

    The UI sends only the fields the user changed (partial update).
    cfg.update(body) merges the new values; existing keys not in body are preserved.

    Data flow:
        Browser form → POST JSON body → cfg.update() → save_config() → config.json

    Returns:
        {"ok": True} on success.
    """
    body = await request.json()   # Parse JSON body from browser
    cfg  = load_config()          # Load current config (with defaults filled in)
    cfg.update(body)              # Merge changes (overwrite changed keys only)
    save_config(cfg)              # Write back to disk
    add_log("✓ Config saved.")    # Show confirmation in the live log
    return {"ok": True}


# =============================================================================
# ROUTES — Status + Logs
# =============================================================================

@app.get("/api/status")
async def get_status():
    """Return a snapshot of the current pipeline state.

    The UI polls this endpoint every 2 seconds to update progress indicators.
    Adding "log_count" lets the UI know how many log lines exist without
    downloading them all — useful to decide whether to show a "new logs" badge.

    Returns:
        All fields from pipeline_state plus "log_count" (number of log lines so far).
    """
    return {**pipeline_state, "log_count": len(pipeline_logs)}


@app.get("/api/logs")
async def get_logs(since: int = 0):
    """Return log lines starting from index `since`.

    The browser can call this to fetch only NEW lines since its last poll.
    Example: first call returns logs[0:], second call passes since=5 to get logs[5:].

    Args:
        since: Index into pipeline_logs (query param, default 0 = all logs)

    Returns:
        {"logs": [{"ts": ..., "msg": ...}, ...], "total": N}
    """
    return {"logs": pipeline_logs[since:], "total": len(pipeline_logs)}


@app.get("/api/logs/stream")
async def stream_logs(request: Request):
    """Stream live log updates to the browser using Server-Sent Events (SSE).

    SSE is a one-way HTTP push protocol: the server keeps the connection open
    and pushes data whenever something happens, without the browser needing to poll.

    How this works:
      1. Browser connects to /api/logs/stream with EventSource API.
      2. The event_generator() coroutine runs inside an async generator.
      3. It waits on log_event (an asyncio.Event) which is set whenever add_log() is called.
      4. When woken, it sends all NEW log lines (tracked by `sent` index) as SSE events.
      5. It also sends a "status" event when pipeline_state["status"] changes.
      6. It sends a "heartbeat" event every second to keep the connection alive
         (some browsers and proxies close idle connections after 30–60 seconds).
      7. The loop repeats until the browser disconnects.

    SSE payload format (text/event-stream protocol):
        data: {"type": "log", "index": 5, "ts": "14:32:07", "msg": "..."}\n\n
        data: {"type": "status", "status": "running_step2", "step": 2}\n\n
        data: {"type": "heartbeat"}\n\n

    Why SSE instead of WebSockets?
      SSE is simpler: one-directional, built into browsers, auto-reconnects.
      WebSockets add bidirectional complexity we don't need here.
    """
    async def event_generator():
        sent        = 0           # How many log entries we have already pushed to this client
        last_status = None        # Last pipeline_state["status"] we pushed to this client
        try:
            while True:
                # Check if the browser has disconnected (tab closed, navigation away, etc.)
                if await request.is_disconnected():
                    break

                # ── Flush any unsent log lines ────────────────────────────────
                while sent < len(pipeline_logs):
                    entry   = pipeline_logs[sent]
                    payload = json.dumps({
                        "type":  "log",
                        "index": sent,           # Client can use this to detect missed events
                        "ts":    entry["ts"],
                        "msg":   entry["msg"],
                    })
                    yield f"data: {payload}\n\n"  # SSE format: "data: ...\n\n"
                    sent += 1

                # ── Push status change if pipeline state changed ───────────────
                current_status = pipeline_state["status"]
                if current_status != last_status:
                    last_status = current_status
                    payload = json.dumps({
                        "type":   "status",
                        "status": current_status,
                        "step":   pipeline_state["current_step"],
                    })
                    yield f"data: {payload}\n\n"

                # ── Heartbeat to keep connection alive ────────────────────────
                yield 'data: {"type":"heartbeat"}\n\n'

                # ── Wait for next event (up to 1 second) ─────────────────────
                # asyncio.wait_for with timeout=1.0 means:
                #   - If add_log() sets log_event within 1 second → wake up immediately
                #   - Otherwise → wake up after 1 second (to send the heartbeat)
                # log_event.clear() resets the event so the next wait blocks again.
                try:
                    await asyncio.wait_for(log_event.wait(), timeout=1.0)
                    log_event.clear()
                except asyncio.TimeoutError:
                    pass    # Normal — just means no new logs in the last second

        except asyncio.CancelledError:
            pass    # Browser disconnected mid-event; clean exit

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",       # Required by SSE spec
        headers={
            "Cache-Control":    "no-cache",   # Tell CDNs/proxies not to buffer responses
            "X-Accel-Buffering": "no",        # Disable nginx's response buffering (common proxy)
            "Connection":       "keep-alive", # Keep the HTTP connection open
        },
    )


# =============================================================================
# SCRIPT RUNNER
# =============================================================================

async def _run_runner(script_name: str) -> int:
    """Launch a runner script (run_step1/2/3.py) as a child process and stream its output.

    This is the core execution engine for pipeline steps. It:
      1. Finds the correct Python executable (venv first, then system Python)
      2. Spawns the script as a subprocess with stdout/stderr pipes
      3. Reads stdout and stderr in background threads (to avoid blocking)
      4. Adds each line to pipeline_logs via add_log()
      5. Awaits process completion without blocking the async event loop

    Why background threads for reading streams?
      subprocess.stdout.readline() is a blocking call. Calling it directly in an
      async coroutine would freeze the event loop. Running it in a thread via
      threading.Thread lets the event loop continue handling other requests.

    Why prefer venv Python?
      The project has a virtual environment at river_env/ with all dependencies.
      Using the venv Python ensures we use the correct package versions (rasterio,
      geopandas, earthengine-api) instead of whatever is globally installed.

    Args:
        script_name: Filename (not path) of the runner, e.g. "run_step1.py"

    Returns:
        int — process exit code (0 = success, non-zero = failure, -1 = launch error)
    """
    global active_process   # Store reference so reset endpoint can kill the process

    script_path = RUNNERS_DIR / script_name    # Full path to the runner

    if not script_path.exists():
        add_log(f"ERROR: Runner not found: {script_path}")
        return -1

    # ── Find Python executable ─────────────────────────────────────────────────
    # On Windows: venv Python is at river_env/Scripts/python.exe
    # On Linux/Mac: venv Python is at river_env/bin/python
    if sys.platform == "win32":
        venv_python = ROOT_DIR / "river_env" / "Scripts" / "python.exe"
    else:
        venv_python = ROOT_DIR / "river_env" / "bin" / "python"

    # Use venv Python if it exists, otherwise fall back to the Python running THIS script
    python_exe = str(venv_python) if venv_python.exists() else sys.executable
    add_log(f"→ Python : {python_exe}")
    add_log(f"→ Script : {script_path.name}")

    # ── Prepare environment ────────────────────────────────────────────────────
    # Copy the current process's environment (inherits PATH, GDAL_DATA, etc.)
    # Set PYTHONIOENCODING so print() in the child process uses UTF-8 on Windows.
    # Without this, Windows uses cp1252 by default, breaking Unicode characters.
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    try:
        # ── Launch the process ─────────────────────────────────────────────────
        # subprocess.Popen with PIPE lets us read stdout/stderr line-by-line.
        # universal_newlines=False → we read raw bytes and decode manually
        # (avoids encoding issues on Windows with cp1252 default codec).
        # cwd=ROOT_DIR so relative paths in the runner scripts resolve correctly.
        process = subprocess.Popen(
            [python_exe, str(script_path)],
            stdout=subprocess.PIPE,           # Capture stdout
            stderr=subprocess.PIPE,           # Capture stderr separately
            cwd=str(ROOT_DIR),                # Working directory = project root
            universal_newlines=False,         # Read as bytes (we decode ourselves)
            env=env,
        )
        active_process = process             # Store for kill-on-reset

        def read_stream(stream, prefix=""):
            """Read a subprocess stream line-by-line and add each line to logs.

            This function runs in a background thread. It blocks on readline()
            until a line is available, decodes it from bytes to str, strips
            trailing whitespace/newlines, and adds it to pipeline_logs.

            Args:
                stream: The subprocess stdout or stderr pipe (bytes mode)
                prefix: Optional prefix to distinguish stderr lines in the UI.
                        stdout lines get no prefix; stderr lines get "[ERR] ".
            """
            for raw in iter(stream.readline, b''):  # b'' = EOF sentinel
                if raw:
                    # Decode bytes → str, replacing unmappable bytes with '?'
                    text = raw.decode("utf-8", errors="replace").rstrip()
                    if text:    # Skip empty lines after stripping
                        add_log(f"{prefix}{text}")

        # Start two threads: one for stdout, one for stderr.
        # daemon=True means these threads die automatically if the main process exits.
        t_out = threading.Thread(target=read_stream, args=(process.stdout,),        daemon=True)
        t_err = threading.Thread(target=read_stream, args=(process.stderr, "[ERR] "), daemon=True)
        t_out.start()
        t_err.start()

        # ── Wait for process to finish (non-blocking) ─────────────────────────
        # run_in_executor() runs process.wait() in a thread pool, releasing
        # the event loop to handle other requests while we wait for the child.
        rc = await asyncio.get_event_loop().run_in_executor(None, process.wait)

        # Give the reader threads 3 seconds to flush any remaining output
        t_out.join(timeout=3.0)
        t_err.join(timeout=3.0)

        add_log(f"→ Process exited: code {rc}")
        return rc   # 0 = success, anything else = failure

    except Exception as exc:
        import traceback
        add_log(f"ERROR: {exc}")
        add_log(traceback.format_exc())
        return -1


# =============================================================================
# ROUTES — Pipeline
# =============================================================================

@app.post("/api/pipeline/reset")
async def reset_pipeline():
    """Stop any running pipeline process and reset state to idle.

    Called when the user clicks "Reset" in the UI.
    Kills the child process (if running), cancels the async task (if running),
    resets pipeline_state back to idle defaults, and clears all logs.

    This allows starting a fresh run without restarting the server.

    Why check active_process.poll()?
      poll() returns None if the process is still running, or the exit code if done.
      We only call kill() if poll() returns None — no point killing an already-dead process.
    """
    global active_process, active_task

    # Cancel the async task wrapping _run_runner()
    if active_task and not active_task.done():
        active_task.cancel()
        active_task = None

    # Kill the subprocess if it's still running
    if active_process:
        try:
            if active_process.poll() is None:   # None means process is still alive
                active_process.kill()            # SIGKILL — immediate termination
        except Exception:
            pass   # Process may have already exited between poll() and kill()
        active_process = None

    # Reset state dict to clean idle state
    pipeline_state.update({
        "status":          "idle",
        "current_step":    0,
        "selected_rivers": [],
        "started_at":      None,
        "error_msg":       None,
    })
    pipeline_logs.clear()    # Wipe all previous log lines
    add_log("Pipeline reset. Ready to start.")
    return {"ok": True}


@app.post("/api/pipeline/step1")
async def start_step1(request: Request):
    """Start Step 1: submit satellite imagery export tasks to Google Earth Engine.

    This endpoint:
      1. Validates that the pipeline isn't already running.
      2. Reads the user's selected rivers from the POST body.
      3. Saves them to config.json so run_step1.py can read them.
      4. Updates pipeline_state to "running_step1".
      5. Launches run_step1.py as an async background task.

    Data flow:
        Browser (river selection) → POST JSON → config.json → run_step1.py → gee_export.py → GEE API

    Why save rivers to config.json instead of passing as args?
      The runner scripts are separate processes that don't share memory with main.py.
      config.json is the shared state file that bridges the two processes.

    Why asyncio.create_task()?
      create_task() schedules _run() to run concurrently in the event loop.
      The HTTP response is returned immediately (non-blocking), while the actual
      GEE export submission happens in the background.

    Returns:
        {"ok": True} immediately — the export runs in the background.
    """
    global active_task

    # Guard: don't start if already running (status must be idle or error to restart)
    if pipeline_state["status"] not in ("idle", "error"):
        raise HTTPException(409, f"Pipeline busy: {pipeline_state['status']}")

    body     = await request.json()
    selected = body.get("rivers", [])
    if not selected:
        raise HTTPException(400, "No rivers selected")

    # Save selected rivers to config.json BEFORE launching the subprocess.
    # run_step1.py reads config.json to get the rivers list.
    cfg = load_config()
    cfg["selected_rivers"] = selected
    save_config(cfg)

    # Update in-memory state immediately so the UI shows the new status
    pipeline_state.update({
        "status":          "running_step1",
        "current_step":    1,
        "selected_rivers": selected,
        "started_at":      datetime.now().isoformat(),
        "error_msg":       None,
    })
    pipeline_logs.clear()   # Fresh log for this run

    # Build a readable preview of selected rivers for the log (first 5, then "+N more")
    preview = ', '.join(selected[:5]) + (f" +{len(selected)-5} more" if len(selected) > 5 else "")
    add_log(f"STEP 1 — GEE Export")
    add_log(f"  Rivers : {preview}")
    add_log(f"  Bands  : {cfg.get('selected_bands')}")
    add_log(f"  Target : {cfg.get('export_target', 'drive').upper()}")

    async def _run():
        """Inner coroutine that runs the step and updates state on completion."""
        rc = await _run_runner("run_step1.py")
        if rc == 0:
            pipeline_state["status"] = "done_step1"
            add_log("✓ Step 1 complete — GEE tasks submitted.")
            add_log("  Monitor: https://code.earthengine.google.com/tasks")
            add_log("  Once all tasks are COMPLETED → click 'Start Download'.")
        else:
            pipeline_state["status"]    = "error"
            pipeline_state["error_msg"] = f"Step 1 exited with code {rc}"
            add_log(f"✗ Step 1 failed (exit code {rc})")
        log_event.set()     # Wake up any SSE clients to push the final status

    active_task = asyncio.create_task(_run())   # Schedule as background coroutine
    return {"ok": True}


@app.post("/api/pipeline/step2")
async def start_step2():
    """Start Step 2: download completed GEE exports from Google Drive.

    Can be started when status is: idle, done_step1, done_step2, or error.
    Why allow "done_step2"? User may want to re-run the download to pick up new files.
    Why allow "idle"? User may want to skip Step 1 and just re-download.

    Data flow:
        run_step2.py → drive_download.py → Google Drive API → local disk (output_base_folder/Sentinel/)

    Returns:
        {"ok": True} immediately — download runs in the background.
    """
    global active_task

    if pipeline_state["status"] not in ("idle", "done_step1", "done_step2", "error"):
        raise HTTPException(409, f"Cannot start Step 2 — status: {pipeline_state['status']}")

    pipeline_state["status"]       = "running_step2"
    pipeline_state["current_step"] = 2
    add_log("STEP 2 — Drive Download")
    add_log("  Existing files skipped, incomplete downloads resumed.")

    async def _run():
        rc = await _run_runner("run_step2.py")
        if rc == 0:
            pipeline_state["status"] = "done_step2"
            add_log("✓ Step 2 complete — files downloaded.")
            add_log("  Click 'Start Merge' to merge the tiles.")
        else:
            pipeline_state["status"]    = "error"
            pipeline_state["error_msg"] = f"Step 2 exited with code {rc}"
            add_log(f"✗ Step 2 failed (exit code {rc})")
        log_event.set()

    active_task = asyncio.create_task(_run())
    return {"ok": True}


@app.post("/api/pipeline/step3")
async def start_step3():
    """Start Step 3: merge downloaded GeoTIFF tiles into one file per river.

    Only allowed when Step 2 has completed (done_step2) or a previous Step 3 errored.
    Why enforce this? The tile files must exist before we can merge them.

    Data flow:
        run_step3.py → merge_tiles.py → reads from Sentinel/ folder → writes to Sentinel_Merged/

    Returns:
        {"ok": True} immediately — merge runs in the background.
    """
    global active_task

    if pipeline_state["status"] not in ("done_step2", "error"):
        raise HTTPException(409, f"Cannot start Step 3 — status: {pipeline_state['status']}")

    pipeline_state["status"]       = "running_step3"
    pipeline_state["current_step"] = 3
    add_log("STEP 3 — Merge GeoTIFF Tiles (local)")

    async def _run():
        rc = await _run_runner("run_step3.py")
        if rc == 0:
            pipeline_state["status"] = "done"
            add_log("✓ Step 3 complete — tiles merged.")
            add_log("🎉 Pipeline finished! Merged files ready for QGIS.")
        else:
            pipeline_state["status"]    = "error"
            pipeline_state["error_msg"] = f"Step 3 exited with code {rc}"
            add_log(f"✗ Step 3 failed (exit code {rc})")
        log_event.set()

    active_task = asyncio.create_task(_run())
    return {"ok": True}


# =============================================================================
# CLOUD VM CONFIG
# =============================================================================
# VM_CONFIG holds the hardcoded GCP parameters for the cloud merge VM.
# These values are also defined in terraform/variables.tf — they must match.
# The VM is an alternative to Step 3 for very large rivers that would run
# out of RAM or storage on a local machine.

VM_CONFIG = {
    "project_id":    "plucky-sight-423703-k5",   # GCP project that owns the VM
    "zone":          "us-central1-a",              # GCP zone where the VM is created
    "vm_name":       "river-merge-vm",             # Name used to query/delete the VM later
    "machine_type":  "n2-highmem-4",               # 4 CPU / 32 GB RAM — enough for most merges
    "disk_size_gb":  "200GB",                       # Boot disk size (needs to fit all tiles)
    "bucket_name":   "aiq-river-imagery",           # GCS bucket with the raw tiles
    "input_prefix":  "Sentinel",                    # GCS folder: gs://aiq-river-imagery/Sentinel/
    "output_prefix": "Sentinel_Merged",             # GCS folder: gs://aiq-river-imagery/Sentinel_Merged/
}


# =============================================================================
# ROUTES — Cloud VM Merge
# =============================================================================

@app.post("/api/vm/launch")
async def launch_merge_vm(request: Request):
    """Launch a GCP Compute Engine VM to perform the tile merge in the cloud.

    This is an alternative to local Step 3 for large datasets that won't fit
    in local RAM or disk space. The VM:
      1. Installs Python dependencies (rasterio, numpy)
      2. Downloads vm_merge_gcs.py from the GCS bucket
      3. Runs the merge, reading tiles from GCS and writing merged files back to GCS
      4. Self-deletes when done (so it doesn't keep billing)

    Why a startup script instead of a Docker image?
      Startup scripts are simpler to iterate on and don't require a container registry.
      The script is embedded directly in the VM metadata — no separate deployment step.

    VM startup script is written to a temp file because `gcloud compute instances create`
    expects the script as a file path (--metadata-from-file). We clean up the temp file
    when gcloud finishes.

    Data flow:
        UI rivers selection → POST body → startup script → gcloud CLI → GCP VM →
        vm_merge_gcs.py → GCS tiles → merged GeoTIFFs uploaded back to GCS

    Returns:
        {"ok": True, "vm_name": "...", "zone": "..."} immediately.
        VM creation runs in the background; check /api/vm/status to monitor.
    """
    global active_task

    body            = await request.json()
    selected_rivers = body.get("rivers", [])

    # ── Guard: don't launch a second VM if one is already running ─────────────
    # Ask gcloud to describe the VM — if it returns status RUNNING/PROVISIONING, abort.
    try:
        check = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: subprocess.run(
                ["gcloud", "compute", "instances", "describe",
                 VM_CONFIG["vm_name"],
                 "--zone",    VM_CONFIG["zone"],
                 "--project", VM_CONFIG["project_id"],
                 "--format=value(status)"],    # Only output the status field
                capture_output=True, text=True, timeout=15
            )
        )
        if check.returncode == 0:
            status = check.stdout.strip()
            if status in ("RUNNING", "PROVISIONING", "STAGING"):
                raise HTTPException(409, f"VM already running (status: {status}). Wait for it to finish.")
    except HTTPException:
        raise   # Re-raise our 409 guard
    except Exception:
        pass    # gcloud returned non-zero = VM doesn't exist yet → safe to proceed

    # ── Build the startup script ───────────────────────────────────────────────
    # rivers_arg is passed to vm_merge_gcs.py's --rivers flag.
    # If empty, the script merges ALL rivers it finds in the bucket.
    rivers_arg = ("--rivers " + " ".join(selected_rivers)) if selected_rivers else ""

    # The f-string uses {{ }} to escape literal braces in the bash script
    # (because this whole string is itself an f-string).
    startup_script = f"""#!/bin/bash
set -e
exec > >(tee -a /var/log/river_merge.log) 2>&1

echo "============================================"
echo "  River Sentinel - Cloud VM Merge"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================"

INSTANCE=$(curl -sf -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name)
ZONE=$(curl -sf -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{{print $NF}}')
PROJECT=$(curl -sf -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id)
echo "Instance: $INSTANCE | Zone: $ZONE"

echo "[1/4] Installing dependencies..."
pip3 install --quiet --upgrade pip
pip3 install --quiet rasterio numpy google-cloud-storage
echo "Done."

echo "[2/4] Downloading merge script from GCS..."
mkdir -p /tmp/river_merge
gsutil cp gs://{VM_CONFIG['bucket_name']}/scripts/vm_merge_gcs.py /tmp/river_merge/vm_merge_gcs.py

echo "[3/4] Running merge..."
python3 /tmp/river_merge/vm_merge_gcs.py \\
  --bucket {VM_CONFIG['bucket_name']} \\
  --input-prefix {VM_CONFIG['input_prefix']} \\
  --output-prefix {VM_CONFIG['output_prefix']} \\
  --work-dir /tmp/river_merge \\
  {rivers_arg}
EXIT_CODE=$?

echo "[4/4] Self-deleting VM (merge exit code: $EXIT_CODE)..."
gcloud compute instances delete "$INSTANCE" --zone="$ZONE" --project="$PROJECT" --quiet
"""

    # ── Write startup script to a temp file ───────────────────────────────────
    # gcloud CLI reads from a file path, not stdin.
    # We write to a NamedTemporaryFile, pass its path to --metadata-from-file,
    # then delete it once gcloud is done.
    tmp_script = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.sh', delete=False, encoding='utf-8'
        ) as f:
            f.write(startup_script)
            tmp_script = f.name   # Store path so we can delete it later

        add_log("☁  Launching GCP merge VM...")
        add_log(f"   Machine  : {VM_CONFIG['machine_type']} (4 CPU / 32 GB RAM)")
        add_log(f"   Zone     : {VM_CONFIG['zone']}")
        add_log(f"   Input    : gs://{VM_CONFIG['bucket_name']}/{VM_CONFIG['input_prefix']}/")
        add_log(f"   Output   : gs://{VM_CONFIG['bucket_name']}/{VM_CONFIG['output_prefix']}/")
        if selected_rivers:
            add_log(f"   Rivers   : {', '.join(selected_rivers)}")
        else:
            add_log("   Rivers   : ALL rivers found in bucket")

        # Build the gcloud CLI command to create the VM
        gcloud_cmd = [
            "gcloud", "compute", "instances", "create", VM_CONFIG["vm_name"],
            "--zone",               VM_CONFIG["zone"],
            "--project",            VM_CONFIG["project_id"],
            "--machine-type",       VM_CONFIG["machine_type"],
            "--image-family",       "debian-12",            # Debian 12 (Bookworm)
            "--image-project",      "debian-cloud",
            "--boot-disk-size",     VM_CONFIG["disk_size_gb"],
            "--boot-disk-type",     "pd-ssd",               # SSD for fast tile reading
            "--scopes",             "storage-full,logging-write,compute-rw",
            # scopes:
            #   storage-full      → VM can read/write GCS bucket
            #   logging-write     → VM can write to Cloud Logging
            #   compute-rw        → VM can delete itself (self-delete step)
            "--metadata-from-file", f"startup-script={tmp_script}",
            "--format",             "json",                 # Output as JSON (easier to parse)
        ]

        async def _create_vm():
            """Run gcloud create in a thread, log results, clean up temp file."""
            try:
                result = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: subprocess.run(gcloud_cmd, capture_output=True, text=True, timeout=120)
                )
                if result.returncode == 0:
                    add_log("✓ VM created and booting!")
                    add_log("  It will self-delete when the merge completes.")
                    add_log(f"  Stream logs: gcloud compute instances get-serial-port-output "
                            f"{VM_CONFIG['vm_name']} --zone={VM_CONFIG['zone']}")
                else:
                    err = (result.stderr or result.stdout).strip()
                    add_log(f"✗ VM creation failed: {err}")
            finally:
                # Always clean up the temp file, even if gcloud failed
                if tmp_script and os.path.exists(tmp_script):
                    os.unlink(tmp_script)
            log_event.set()

        active_task = asyncio.create_task(_create_vm())
        return {"ok": True, "vm_name": VM_CONFIG["vm_name"], "zone": VM_CONFIG["zone"]}

    except Exception as e:
        if tmp_script and os.path.exists(tmp_script):
            os.unlink(tmp_script)
        raise HTTPException(500, str(e))


@app.get("/api/vm/status")
async def get_vm_status():
    """Check if the merge VM is still alive in GCP.

    The UI polls this every 30 seconds while the VM is running.
    When the VM finishes the merge, it self-deletes → gcloud describe returns non-zero
    → this endpoint returns {"exists": False, "status": "DELETED"}.

    Why does the VM self-delete?
      A VM that's done running costs money even if idle. The startup script
      runs `gcloud compute instances delete` as its last step.

    Returns:
        {
          "exists":  bool,        # True if the VM still exists
          "status":  str,         # "RUNNING", "STAGING", "DELETED", "UNKNOWN", etc.
          "running": bool,        # True if actively processing
        }
    """
    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: subprocess.run(
                ["gcloud", "compute", "instances", "describe",
                 VM_CONFIG["vm_name"],
                 "--zone",    VM_CONFIG["zone"],
                 "--project", VM_CONFIG["project_id"],
                 "--format=value(status)"],     # Only want the status string
                capture_output=True, text=True, timeout=15
            )
        )
        if result.returncode == 0:
            status = result.stdout.strip()
            return {
                "exists":  True,
                "status":  status,
                "running": status in ("RUNNING", "PROVISIONING", "STAGING"),
            }
        # Non-zero return = VM not found = it finished and self-deleted
        return {"exists": False, "status": "DELETED", "running": False}
    except Exception as e:
        return {"exists": False, "status": "UNKNOWN", "running": False, "error": str(e)}


# =============================================================================
# ROUTES — DEM Clean
# =============================================================================
# The DEM (Digital Elevation Model) cleaning tool is a standalone processing step
# that the user can trigger independently from the main 3-step pipeline.
# It runs scripts/dem_clean.py on a local GeoTIFF file.
#
# Separate state (dem_clean_state, dem_clean_logs) is used so DEM cleaning
# doesn't interfere with or overwrite the main pipeline's logs/status.

dem_clean_state: dict = {
    "status":    "idle",    # "idle" | "running" | "done" | "error"
    "error_msg": None,      # Set when status == "error"
}
dem_clean_logs: list[dict] = []              # Log entries for DEM clean (same format as pipeline_logs)
dem_clean_task: Optional[asyncio.Task] = None  # Background task reference for cancellation


@app.get("/api/dem-clean/status")
async def get_dem_clean_status():
    """Return current DEM clean state plus log count.

    The UI polls this while a DEM clean is running.
    """
    return {**dem_clean_state, "log_count": len(dem_clean_logs)}


@app.get("/api/dem-clean/logs")
async def get_dem_clean_logs(since: int = 0):
    """Return DEM clean log entries from index `since` onward.

    Same pattern as /api/logs — client passes its last-seen index to get only new lines.
    """
    return {"logs": dem_clean_logs[since:], "total": len(dem_clean_logs)}


@app.post("/api/dem-clean")
async def run_dem_clean(request: Request):
    """Start the DEM cleaning tool on a local GeoTIFF file.

    The user provides input_path (source GeoTIFF) and output_path (cleaned output).
    The tool auto-detects whether the file is a DEM or Sentinel-2 image and
    applies the appropriate cleaning rules (remove NoData, NaN, zero/negative pixels).

    Data flow:
        POST body {input_path, output_path} → dem_clean.py (subprocess) → cleaned .tif

    Why a subprocess instead of calling dem_clean functions directly?
      dem_clean.py uses module-level code (not wrapped in a function) and
      uses argparse. Running it as a subprocess is cleaner than hacking around that.

    Returns:
        {"ok": True} immediately — cleaning runs in the background.
    """
    global dem_clean_task

    if dem_clean_state["status"] == "running":
        raise HTTPException(409, "DEM Clean is already running.")

    body       = await request.json()
    input_path = body.get("input_path", "").strip()
    output_path = body.get("output_path", "").strip()

    # Validate inputs before starting the subprocess
    if not input_path:
        raise HTTPException(400, "input_path is required.")
    if not output_path:
        raise HTTPException(400, "output_path is required.")
    if not os.path.exists(input_path):
        raise HTTPException(400, f"Input file not found: {input_path}")

    # Create output directory if it doesn't exist
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    # Verify the dem_clean.py script exists
    script_path = ROOT_DIR / "scripts" / "dem_clean.py"
    if not script_path.exists():
        raise HTTPException(500, f"Script not found: {script_path}")

    # Find venv Python (same logic as _run_runner)
    if sys.platform == "win32":
        venv_python = ROOT_DIR / "river_env" / "Scripts" / "python.exe"
    else:
        venv_python = ROOT_DIR / "river_env" / "bin" / "python"
    python_exe = str(venv_python) if venv_python.exists() else sys.executable

    # Update state and clear old logs
    dem_clean_state.update({"status": "running", "error_msg": None})
    dem_clean_logs.clear()

    def _add_dem_log(msg: str):
        """Add a log entry to BOTH dem_clean_logs and the main pipeline_logs.

        Why mirror to main logs? So the user can see DEM clean progress in the
        main live console even if they're not on the DEM Clean tab.
        """
        ts = datetime.now().strftime("%H:%M:%S")
        dem_clean_logs.append({"ts": ts, "msg": msg})
        add_log(msg)    # Also mirror to the main pipeline log

    _add_dem_log("DEM CLEAN — Starting")
    _add_dem_log(f"  Input  : {input_path}")
    _add_dem_log(f"  Output : {output_path}")

    async def _run():
        """Launch dem_clean.py as a subprocess with input/output paths as arguments."""
        global active_process
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        try:
            # Pass input_path and output_path as positional CLI arguments.
            # dem_clean.py's argparse picks them up as input_dem and output_dem.
            process = subprocess.Popen(
                [python_exe, str(script_path), input_path, output_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=str(ROOT_DIR),
                universal_newlines=False,
                env=env,
            )
            active_process = process

            def read_stream(stream, prefix=""):
                for raw in iter(stream.readline, b''):
                    if raw:
                        text = raw.decode("utf-8", errors="replace").rstrip()
                        if text:
                            _add_dem_log(f"{prefix}{text}")

            # Two background threads: one for stdout, one for stderr
            t_out = threading.Thread(target=read_stream, args=(process.stdout,), daemon=True)
            t_err = threading.Thread(target=read_stream, args=(process.stderr, "[ERR] "), daemon=True)
            t_out.start(); t_err.start()

            # Wait for completion without blocking the event loop
            rc = await asyncio.get_event_loop().run_in_executor(None, process.wait)
            t_out.join(timeout=3.0); t_err.join(timeout=3.0)

            if rc == 0:
                dem_clean_state["status"] = "done"
                _add_dem_log(f"✅ DEM Clean complete → {output_path}")
            else:
                dem_clean_state["status"] = "error"
                dem_clean_state["error_msg"] = f"Exited with code {rc}"
                _add_dem_log(f"✗ DEM Clean failed (exit code {rc})")
        except Exception as exc:
            import traceback
            dem_clean_state["status"] = "error"
            dem_clean_state["error_msg"] = str(exc)
            _add_dem_log(f"ERROR: {exc}")
            _add_dem_log(traceback.format_exc())
        finally:
            log_event.set()     # Wake up SSE clients to push final status

    dem_clean_task = asyncio.create_task(_run())
    return {"ok": True}


@app.post("/api/dem-clean/reset")
async def reset_dem_clean():
    """Stop any running DEM clean and reset its state to idle.

    Cancels the async task and kills the subprocess if running.
    Called when user clicks "Reset" on the DEM Clean panel.
    """
    global dem_clean_task, active_process
    if dem_clean_task and not dem_clean_task.done():
        dem_clean_task.cancel()
    if active_process and active_process.poll() is None:
        try:
            active_process.kill()
        except Exception:
            pass
    dem_clean_state.update({"status": "idle", "error_msg": None})
    dem_clean_logs.clear()
    add_log("DEM Clean reset.")
    return {"ok": True}


@app.delete("/api/vm/kill")
async def kill_merge_vm():
    """Emergency: force-delete the merge VM via gcloud CLI.

    Called when the user clicks "Kill VM" in the UI.
    Normally the VM self-deletes, but if it hangs or errors before the
    self-delete step, this endpoint provides manual cleanup.

    Uses gcloud compute instances delete with --quiet to skip confirmation prompt.

    Returns:
        {"ok": True} on successful deletion.
        Raises HTTPException(500) if gcloud fails.
    """
    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: subprocess.run(
                ["gcloud", "compute", "instances", "delete",
                 VM_CONFIG["vm_name"],
                 "--zone",    VM_CONFIG["zone"],
                 "--project", VM_CONFIG["project_id"],
                 "--quiet"],        # Skip "are you sure?" confirmation
                capture_output=True, text=True, timeout=60
            )
        )
        if result.returncode == 0:
            add_log("☁  VM force-deleted by user.")
            return {"ok": True}
        raise HTTPException(500, f"Delete failed: {result.stderr.strip()}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


# =============================================================================
# ROUTES — Terraform
# =============================================================================
# Terraform is used to provision cloud infrastructure: IAM roles and the GCP VM.
# The terraform/ folder contains HCL configuration files.
# These API endpoints let the user run `terraform apply` and `terraform destroy`
# from the browser, with real-time log streaming.
#
# Why Terraform instead of just gcloud CLI?
#   Terraform tracks state, so it can destroy exactly what it created.
#   It also handles IAM member bindings declaratively (idempotent).

TERRAFORM_DIR = ROOT_DIR / "terraform"  # Path to the terraform/ subfolder

terraform_state: dict = {
    "status":    "idle",    # "idle" | "running" | "done" | "destroyed" | "error"
    "last_log":  "",        # Last log line (used by UI for quick status display)
    "error_msg": None,      # Set when status == "error"
}
terraform_logs: list[dict] = []              # Log entries for terraform operations
terraform_task: Optional[asyncio.Task] = None  # Background task reference


def _add_tf_log(msg: str):
    """Add a log entry to terraform_logs AND mirror it to the main pipeline_logs.

    Also updates terraform_state["last_log"] for the quick-status display in the UI.
    """
    ts = datetime.now().strftime("%H:%M:%S")
    terraform_logs.append({"ts": ts, "msg": msg})
    terraform_state["last_log"] = msg
    add_log(msg)   # Mirror to main live console so user sees it everywhere


@app.get("/api/terraform/status")
async def get_tf_status():
    """Return Terraform operation state plus log count.

    Polled by the Terraform panel in the UI.
    """
    return {**terraform_state, "log_count": len(terraform_logs)}


@app.get("/api/terraform/logs")
async def get_tf_logs(since: int = 0):
    """Return Terraform log entries from index `since` onward.

    Same pagination pattern as /api/logs.
    """
    return {"logs": terraform_logs[since:], "total": len(terraform_logs)}


async def _run_terraform(cmd_args: list[str], success_status: str) -> None:
    """Core Terraform runner: executes `terraform <cmd_args>` and streams output.

    This function is shared by both /api/terraform/apply and /api/terraform/destroy.
    It always runs `terraform init` first (safe to re-run, idempotent), then the
    requested command.

    Why always init first?
      terraform init downloads provider plugins (.terraform/ folder).
      If the folder is missing (first run or clean checkout), apply/destroy would fail.
      Re-running init when plugins are already cached is a no-op (very fast).

    Key environment variables set for Terraform:
      TF_IN_AUTOMATION=1      — suppresses interactive prompts (e.g. "Enter a value")
      TF_CLI_ARGS=-no-color   — disables ANSI colors in output (we strip them anyway)
      PYTHONIOENCODING=utf-8  — not used by Terraform, but kept for consistency

    Args:
        cmd_args:       List of Terraform CLI arguments, e.g. ["apply", "-auto-approve", "-input=false"]
        success_status: What to set terraform_state["status"] to on success ("done" or "destroyed")
    """
    global active_process

    if not TERRAFORM_DIR.exists():
        _add_tf_log(f"ERROR: terraform/ directory not found at {TERRAFORM_DIR}")
        terraform_state["status"] = "error"
        terraform_state["error_msg"] = "terraform/ directory not found"
        log_event.set()
        return

    # Find terraform binary in PATH. shutil.which() searches PATH like a shell would.
    # Falls back to "terraform" (let the OS error if not found) if which() returns None.
    tf_exe = shutil.which("terraform") or "terraform"

    env = os.environ.copy()
    env["TF_IN_AUTOMATION"] = "1"          # Non-interactive mode
    env["TF_CLI_ARGS"]      = "-no-color"  # No ANSI colors
    env["PYTHONIOENCODING"] = "utf-8"

    async def _execute(args):
        """Run one terraform command and stream its output to logs.

        Returns the exit code.

        Why merge stderr into stdout?
          Terraform writes all its output (including errors) to stdout by convention.
          Merging with stderr=STDOUT ensures we capture everything in one stream.
        """
        _add_tf_log(f"→ Running: terraform {' '.join(args)}")
        process = subprocess.Popen(
            [tf_exe] + args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,   # Merge stderr into stdout (Terraform convention)
            cwd=str(TERRAFORM_DIR),     # Run from terraform/ so .terraform/ folder is found
            universal_newlines=False,
            env=env,
        )
        active_process = process

        def _read():
            """Read terraform stdout line by line, strip ANSI codes, add to logs."""
            for raw in iter(process.stdout.readline, b''):
                if raw:
                    text = raw.decode("utf-8", errors="replace").rstrip()
                    text = _strip_ansi(text)   # Remove leftover ANSI codes (TF_CLI_ARGS should prevent these)
                    if text:
                        _add_tf_log(text)

        t = threading.Thread(target=_read, daemon=True)
        t.start()
        rc = await asyncio.get_event_loop().run_in_executor(None, process.wait)
        t.join(timeout=5.0)
        return rc

    try:
        # Always init first
        rc_init = await _execute(["init", "-input=false"])
        if rc_init != 0:
            raise RuntimeError(f"terraform init failed (exit {rc_init})")

        # Run the main command (apply or destroy)
        rc = await _execute(cmd_args)
        if rc == 0:
            terraform_state["status"] = success_status
            _add_tf_log(f"✅ terraform {cmd_args[0]} complete.")
        else:
            raise RuntimeError(f"terraform {cmd_args[0]} exited with code {rc}")

    except Exception as exc:
        import traceback
        terraform_state["status"] = "error"
        terraform_state["error_msg"] = str(exc)
        _add_tf_log(f"✗ {exc}")
        _add_tf_log(traceback.format_exc())
    finally:
        log_event.set()     # Wake up SSE clients


@app.post("/api/terraform/apply")
async def terraform_apply():
    """Run `terraform apply -auto-approve` in a background task.

    -auto-approve skips the "Do you want to perform these actions?" confirmation.
    -input=false prevents Terraform from blocking on stdin for variable prompts.

    What does apply do here?
      Creates the IAM bindings (GCS bucket access, self-delete permission) and
      launches the GCP Compute Engine VM as defined in terraform/main.tf.

    Returns:
        {"ok": True} immediately.
    """
    global terraform_task
    if terraform_state["status"] == "running":
        raise HTTPException(409, "Terraform is already running.")

    terraform_state.update({"status": "running", "last_log": "", "error_msg": None})
    terraform_logs.clear()
    _add_tf_log("TERRAFORM — apply -auto-approve")

    terraform_task = asyncio.create_task(
        _run_terraform(["apply", "-auto-approve", "-input=false"], success_status="done")
    )
    return {"ok": True}


@app.post("/api/terraform/destroy")
async def terraform_destroy():
    """Run `terraform destroy -auto-approve` to tear down all provisioned resources.

    What does destroy do here?
      Removes the IAM bindings and deletes the GCP VM (if it still exists).
      Safe to run even after the VM has self-deleted — Terraform handles
      "already deleted" gracefully.

    Returns:
        {"ok": True} immediately.
    """
    global terraform_task
    if terraform_state["status"] == "running":
        raise HTTPException(409, "Terraform is already running.")

    terraform_state.update({"status": "running", "last_log": "", "error_msg": None})
    terraform_logs.clear()
    _add_tf_log("TERRAFORM — destroy -auto-approve")

    terraform_task = asyncio.create_task(
        _run_terraform(["destroy", "-auto-approve", "-input=false"], success_status="destroyed")
    )
    return {"ok": True}


# =============================================================================
# ENTRY POINT
# =============================================================================
# When this file is run directly (`python main.py`) instead of via uvicorn CLI,
# this block starts the uvicorn ASGI server programmatically.
# In production, you would typically use:
#   uvicorn main:app --host 0.0.0.0 --port 8000
# The `reload=True` hot-reloads the server when source files change — useful in dev.

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)