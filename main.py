"""
River Sentinel — FastAPI Dashboard Backend
==========================================
Manages the 3-step remote sensing pipeline:
  Step 1: GEE Export (submit tasks to Google Earth Engine)
  Step 2: Drive Download (download completed files from Google Drive)
  Step 3: Merge Tiles (merge raw GeoTIFFs into single file per river)

Run:
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse
import asyncio
import json
import os
import sys
import time
import subprocess
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

# ── Paths ─────────────────────────────────────────────────────────────────────

ROOT_DIR      = Path(__file__).parent
CONFIG_FILE   = ROOT_DIR / "config.json"
RUNNERS_DIR   = ROOT_DIR / "runners"
TEMPLATES_DIR = ROOT_DIR / "templates"

# ── State ─────────────────────────────────────────────────────────────────────

pipeline_state: dict = {
    "status": "idle",          # idle | running_step1 | done_step1 | running_step2 | done_step2 | running_step3 | done | error
    "current_step": 0,
    "selected_rivers": [],
    "started_at": None,
    "error_msg": None,
}

pipeline_logs: list[dict] = []   # [{"ts": "HH:MM:SS", "msg": "..."}]
log_event = asyncio.Event()      # fires when new logs are added
active_process: Optional[asyncio.subprocess.Process] = None  # type: ignore

# ── Config helpers ─────────────────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "shapefile_path":       "",
    "output_base_folder":   "",
    "sentinel_subfolder":   "Sentinel",
    "dem_subfolder":        "DEM",
    "credentials_file":     "",
    "token_file":           "",
    "drive_sentinel_folder": "River_Imagery_Batch/Sentinel",
    "drive_dem_folder":     "River_Imagery_Batch/DEM",
    "gee_project":          "plucky-sight-423703-k5",
    "buffer_distance":      10000,
    "resolution":           10,
    "max_cloud_cover":      10,
    "drive_folder":         "River_Imagery_Batch",
    "max_concurrent_tasks": 100,
    "selected_rivers":      [],
    "skip_existing":        True,
    "start_date":           "2025-01-01",
    "end_date":             "2025-12-31",
}

def load_config() -> dict:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            cfg = json.load(f)
        # Fill in any missing keys from defaults
        for k, v in DEFAULT_CONFIG.items():
            cfg.setdefault(k, v)
        return cfg
    return dict(DEFAULT_CONFIG)

def save_config(cfg: dict):
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)

# ── Log helpers ────────────────────────────────────────────────────────────────

def add_log(message: str):
    ts = datetime.now().strftime("%H:%M:%S")
    pipeline_logs.append({"ts": ts, "msg": message})
    log_event.set()

# ── River list ────────────────────────────────────────────────────────────────

FALLBACK_RIVERS = sorted(set([
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
    "Bhima", "Tunga", "Kumudvathi", "Hagari", "Moyar",
    "Cheyyar", "Vegavathy", "Palar", "Chittar", "Neyyar",
    "Kodayar", "Kallar", "Ullhas", "Savitri", "Vashishti",
    "Dhadhar", "Watrak", "Rupen", "Banas", "Luni",
    "Torsa", "Jaldhaka", "Raidak", "Piyain", "Gaula",
    "Bagmati", "Kamla", "Balan", "Burhi Gandak", "Kiul",
    "Punpun", "Falgu", "Sone", "Rihand", "Kanhar",
    "Johilla", "Tawa", "Denwa", "Shakkar", "Sher",
]))

# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(title="River Sentinel Dashboard")

# ── Routes: Pages ─────────────────────────────────────────────────────────────

@app.get("/")
async def dashboard():
    html_file = TEMPLATES_DIR / "index.html"
    return HTMLResponse(html_file.read_text(encoding="utf-8"))

# ── Routes: Rivers ────────────────────────────────────────────────────────────

@app.get("/api/rivers")
async def get_rivers():
    cfg = load_config()
    shapefile = cfg.get("shapefile_path", "")
    rivers = []
    source = "fallback"

    if shapefile and os.path.exists(shapefile):
        try:
            import geopandas as gpd
            gdf = gpd.read_file(shapefile)
            for col in ["rivname", "name", "NAME", "RIVNAME", "RiverName", "River_Name"]:
                if col in gdf.columns:
                    rivers = sorted(gdf[col].dropna().unique().tolist())
                    source = "shapefile"
                    break
            if rivers:
                add_log(f"OK: Loaded {len(rivers)} rivers from shapefile")
            else:
                add_log(f"WARN: Shapefile loaded but no recognized name column found. Using fallback list.")
        except Exception as e:
            add_log(f"WARN: Shapefile read error: {e}")

    if not rivers:
        rivers = list(FALLBACK_RIVERS)

    return {"rivers": rivers, "count": len(rivers), "source": source}

# ── Routes: Config ────────────────────────────────────────────────────────────

@app.get("/api/config")
async def get_config():
    return load_config()

@app.post("/api/config")
async def update_config(request: Request):
    body = await request.json()
    cfg = load_config()
    cfg.update(body)
    save_config(cfg)
    add_log(f"OK: Config saved.")
    return {"ok": True}

# ── Routes: Status ────────────────────────────────────────────────────────────

@app.get("/api/status")
async def get_status():
    return {**pipeline_state, "log_count": len(pipeline_logs)}

# ── Routes: Logs ──────────────────────────────────────────────────────────────

@app.get("/api/logs")
async def get_logs(since: int = 0):
    return {"logs": pipeline_logs[since:], "total": len(pipeline_logs)}

@app.get("/api/logs/stream")
async def stream_logs(request: Request):
    """Server-Sent Events endpoint for real-time log streaming."""
    async def event_generator():
        sent = 0
        last_status = None
        try:
            while True:
                if await request.is_disconnected():
                    break

                # Drain any new log lines
                while sent < len(pipeline_logs):
                    entry = pipeline_logs[sent]
                    payload = json.dumps({
                        "type": "log",
                        "index": sent,
                        "ts": entry["ts"],
                        "msg": entry["msg"],
                    })
                    yield f"data: {payload}\n\n"
                    sent += 1

                # Send status update if changed
                current_status = pipeline_state["status"]
                if current_status != last_status:
                    last_status = current_status
                    payload = json.dumps({
                        "type": "status",
                        "status": current_status,
                        "step": pipeline_state["current_step"],
                    })
                    yield f"data: {payload}\n\n"

                # Heartbeat
                yield "data: {\"type\":\"heartbeat\"}\n\n"

                # Wait for new events (max 1s)
                try:
                    await asyncio.wait_for(log_event.wait(), timeout=1.0)
                    log_event.clear()
                except asyncio.TimeoutError:
                    pass

        except asyncio.CancelledError:
            pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )

# ── Script runner ─────────────────────────────────────────────────────────────

async def _run_runner(script_name: str):
    """Run a runner script using subprocess.Popen in a thread, streaming output to pipeline_logs."""
    global active_process
    script_path = RUNNERS_DIR / script_name

    if not script_path.exists():
        add_log(f"ERROR: Runner not found: {script_path}")
        return -1

    # Use the virtual environment's Python if available
    if sys.platform == "win32":
        venv_python = ROOT_DIR / "river_env" / "Scripts" / "python.exe"
    else:
        venv_python = ROOT_DIR / "river_env" / "bin" / "python"
    
    if venv_python.exists():
        python_exe = str(venv_python)
        add_log(f"Using venv Python: {python_exe}")
    else:
        python_exe = sys.executable
        add_log(f"Venv not found, using system Python: {python_exe}")
    
    cmd = [python_exe, str(script_path)]
    add_log(f"Command: {' '.join(cmd)}")
    add_log(f"Working dir: {ROOT_DIR}")

    try:
        # Use subprocess.Popen instead of asyncio subprocess (more reliable on Windows)
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"  # Fix Unicode encoding issues on Windows
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(ROOT_DIR),
            universal_newlines=False,  # Get bytes, decode manually
            env=env,
        )
        
        active_process = process
        
        # Read streams in a thread to avoid blocking
        def read_output():
            """Read stdout and stderr, log each line."""
            # Read stdout
            if process.stdout:
                for line_bytes in iter(process.stdout.readline, b''):
                    if line_bytes:
                        try:
                            text = line_bytes.decode("utf-8", errors="replace").rstrip()
                            if text:
                                add_log(text)
                        except Exception as e:
                            add_log(f"Error decoding stdout: {e}")
            
            # Read stderr
            if process.stderr:
                for line_bytes in iter(process.stderr.readline, b''):
                    if line_bytes:
                        try:
                            text = line_bytes.decode("utf-8", errors="replace").rstrip()
                            if text:
                                add_log(f"[STDERR] {text}")
                        except Exception as e:
                            add_log(f"Error decoding stderr: {e}")
        
        # Start reading in a background thread
        reader_thread = threading.Thread(target=read_output, daemon=True)
        reader_thread.start()
        
        # Wait for process in the event loop
        loop = asyncio.get_event_loop()
        rc = await loop.run_in_executor(None, process.wait)
        
        # Wait a bit for reader thread to finish
        reader_thread.join(timeout=2.0)
        
        add_log(f"Process exited with code: {rc}")
        return rc

    except Exception as exc:
        import traceback
        add_log(f"ERROR: Subprocess error: {exc}")
        add_log(traceback.format_exc())
        return -1

# ── Routes: Pipeline Steps ────────────────────────────────────────────────────

@app.post("/api/pipeline/reset")
async def reset_pipeline():
    global active_process
    if active_process:
        try:
            if active_process.poll() is None:  # Still running
                active_process.kill()
        except Exception:
            pass
        active_process = None

    pipeline_state.update({
        "status": "idle",
        "current_step": 0,
        "selected_rivers": [],
        "started_at": None,
        "error_msg": None,
    })
    pipeline_logs.clear()
    add_log("Pipeline reset. Ready to start.")
    return {"ok": True}


@app.post("/api/pipeline/step1")
async def start_step1(request: Request):
    if pipeline_state["status"] not in ("idle", "error"):
        raise HTTPException(409, f"Pipeline busy: {pipeline_state['status']}")

    body = await request.json()
    selected = body.get("rivers", [])
    if not selected:
        raise HTTPException(400, "No rivers selected")

    # Persist selection into config so runner can read it
    cfg = load_config()
    cfg["selected_rivers"] = selected
    save_config(cfg)

    pipeline_state.update({
        "status": "running_step1",
        "current_step": 1,
        "selected_rivers": selected,
        "started_at": datetime.now().isoformat(),
        "error_msg": None,
    })
    pipeline_logs.clear()

    add_log(f"STEP 1: GEE Export")
    add_log(f"  {len(selected)} river(s): {', '.join(selected[:5])}" +
            (f" ... +{len(selected)-5} more" if len(selected) > 5 else ""))

    async def _run():
        rc = await _run_runner("run_step1.py")
        if rc == 0:
            pipeline_state["status"] = "done_step1"
            add_log("OK: Step 1 complete - GEE tasks submitted to Google Drive.")
            add_log("Open https://code.earthengine.google.com/tasks")
            add_log("Wait until all tasks show COMPLETED, then click 'Start Download'.")
        else:
            pipeline_state["status"] = "error"
            pipeline_state["error_msg"] = f"Step 1 exited with code {rc}"
            add_log(f"ERROR: Step 1 failed (exit code {rc})")
        log_event.set()

    asyncio.create_task(_run())
    return {"ok": True}


@app.post("/api/pipeline/step2")
async def start_step2():
    if pipeline_state["status"] not in ("idle", "done_step1", "done_step2", "error"):
        raise HTTPException(409, f"Cannot start Step 2 — current status: {pipeline_state['status']}")

    pipeline_state["status"] = "running_step2"
    pipeline_state["current_step"] = 2
    add_log("STEP 2: Downloading files from Google Drive...")
    add_log("(Existing files will be skipped, incomplete downloads will resume)")

    async def _run():
        rc = await _run_runner("run_step2.py")
        if rc == 0:
            pipeline_state["status"] = "done_step2"
            add_log("OK: Step 2 complete - files downloaded to local machine.")
            add_log("Click 'Start Merge' to merge the tiles.")
        else:
            pipeline_state["status"] = "error"
            pipeline_state["error_msg"] = f"Step 2 exited with code {rc}"
            add_log(f"ERROR: Step 2 failed (exit code {rc})")
        log_event.set()

    asyncio.create_task(_run())
    return {"ok": True}


@app.post("/api/pipeline/step3")
async def start_step3():
    if pipeline_state["status"] not in ("done_step2", "error"):
        raise HTTPException(409, f"Cannot start Step 3 — current status: {pipeline_state['status']}")

    pipeline_state["status"] = "running_step3"
    pipeline_state["current_step"] = 3
    add_log("STEP 3: Merging GeoTIFF tiles...")

    async def _run():
        rc = await _run_runner("run_step3.py")
        if rc == 0:
            pipeline_state["status"] = "done"
            add_log("OK: Step 3 complete - all tiles merged.")
            add_log("SUCCESS: Pipeline finished! Merged files ready for QGIS.")
        else:
            pipeline_state["status"] = "error"
            pipeline_state["error_msg"] = f"Step 3 exited with code {rc}"
            add_log(f"ERROR: Step 3 failed (exit code {rc})")
        log_event.set()

    asyncio.create_task(_run())
    return {"ok": True}


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)