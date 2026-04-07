"""
River Sentinel — FastAPI Dashboard Backend
==========================================
Manages the 3-step remote sensing pipeline:
  Step 1: GEE Export  — submit tasks to Google Earth Engine
  Step 2: Drive Download — download completed files from Google Drive
  Step 3: Merge Tiles — merge raw GeoTIFFs into single file per river

Run:
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse
from contextlib import asynccontextmanager
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

# =============================================================================
# PATHS
# =============================================================================

ROOT_DIR      = Path(__file__).parent
CONFIG_FILE   = ROOT_DIR / "config.json"
RUNNERS_DIR   = ROOT_DIR / "runners"
TEMPLATES_DIR = ROOT_DIR / "templates"

# =============================================================================
# STATE
# =============================================================================

pipeline_state: dict = {
    "status":          "idle",
    "current_step":    0,
    "selected_rivers": [],
    "started_at":      None,
    "error_msg":       None,
}

pipeline_logs: list[dict] = []
log_event:     asyncio.Event = None          # initialised in lifespan
active_process: Optional[subprocess.Popen] = None
active_task:    Optional[asyncio.Task]     = None

# =============================================================================
# CONFIG
# =============================================================================

DEFAULT_CONFIG = {
    # Paths
    "shapefile_path":       "",
    "output_base_folder":   "",
    "sentinel_subfolder":   "Sentinel",
    "dem_subfolder":        "DEM",
    # Drive auth
    "credentials_file":     "",
    "token_file":           "",
    # Drive folders
    "drive_folder":         "River_Imagery_Batch",
    "drive_sentinel_folder": "River_Imagery_Batch/Sentinel",
    "drive_dem_folder":     "River_Imagery_Batch/DEM",
    # GEE
    "gee_project":          "plucky-sight-423703-k5",
    # Export settings
    "export_target":        "drive",          # drive | gcs | both
    "selected_bands":       ["B2", "B3", "B4", "B5", "B6", "B7", "B8", "B8A", "B11", "B12"],
    # Processing
    "buffer_distance":      10000,
    "resolution":           10,
    "max_cloud_cover":      10,
    "max_concurrent_tasks": 100,
    "skip_existing":        True,
    "start_date":           "2025-01-01",
    "end_date":             "2025-12-31",
    # Runtime (not user-editable via UI)
    "selected_rivers":      [],
}

def load_config() -> dict:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            cfg = json.load(f)
        for k, v in DEFAULT_CONFIG.items():
            cfg.setdefault(k, v)
        return cfg
    return dict(DEFAULT_CONFIG)

def save_config(cfg: dict):
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)

# =============================================================================
# LOGGING
# =============================================================================

def add_log(message: str):
    ts = datetime.now().strftime("%H:%M:%S")
    pipeline_logs.append({"ts": ts, "msg": message})
    if log_event:
        log_event.set()

# =============================================================================
# RIVER LIST
# =============================================================================

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    global log_event
    log_event = asyncio.Event()
    add_log("River Sentinel dashboard ready.")
    yield

app = FastAPI(title="River Sentinel Dashboard", lifespan=lifespan)

# =============================================================================
# ROUTES — Pages
# =============================================================================

@app.get("/")
async def dashboard():
    html_file = TEMPLATES_DIR / "index.html"
    return HTMLResponse(html_file.read_text(encoding="utf-8"))

# =============================================================================
# ROUTES — Rivers
# =============================================================================

@app.get("/api/rivers")
async def get_rivers():
    cfg      = load_config()
    shapefile = cfg.get("shapefile_path", "")
    rivers   = []
    source   = "fallback"

    if shapefile and os.path.exists(shapefile):
        try:
            import geopandas as gpd
            gdf = gpd.read_file(shapefile)
            for col in ("rivname", "name", "NAME", "RIVNAME", "RiverName", "River_Name"):
                if col in gdf.columns:
                    rivers = sorted(gdf[col].dropna().unique().tolist())
                    source = "shapefile"
                    break
        except Exception as e:
            add_log(f"WARN: Shapefile read error: {e}")

    if not rivers:
        rivers = list(FALLBACK_RIVERS)

    return {"rivers": rivers, "count": len(rivers), "source": source}

# =============================================================================
# ROUTES — Config
# =============================================================================

@app.get("/api/config")
async def get_config():
    return load_config()

@app.post("/api/config")
async def update_config(request: Request):
    body = await request.json()
    cfg  = load_config()
    cfg.update(body)
    save_config(cfg)
    add_log("✓ Config saved.")
    return {"ok": True}

# =============================================================================
# ROUTES — Status + Logs
# =============================================================================

@app.get("/api/status")
async def get_status():
    return {**pipeline_state, "log_count": len(pipeline_logs)}

@app.get("/api/logs")
async def get_logs(since: int = 0):
    return {"logs": pipeline_logs[since:], "total": len(pipeline_logs)}

@app.get("/api/logs/stream")
async def stream_logs(request: Request):
    async def event_generator():
        sent        = 0
        last_status = None
        try:
            while True:
                if await request.is_disconnected():
                    break

                while sent < len(pipeline_logs):
                    entry   = pipeline_logs[sent]
                    payload = json.dumps({
                        "type":  "log",
                        "index": sent,
                        "ts":    entry["ts"],
                        "msg":   entry["msg"],
                    })
                    yield f"data: {payload}\n\n"
                    sent += 1

                current_status = pipeline_state["status"]
                if current_status != last_status:
                    last_status = current_status
                    payload = json.dumps({
                        "type":   "status",
                        "status": current_status,
                        "step":   pipeline_state["current_step"],
                    })
                    yield f"data: {payload}\n\n"

                yield 'data: {"type":"heartbeat"}\n\n'

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
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":       "keep-alive",
        },
    )

# =============================================================================
# SCRIPT RUNNER
# =============================================================================

async def _run_runner(script_name: str) -> int:
    global active_process

    script_path = RUNNERS_DIR / script_name
    if not script_path.exists():
        add_log(f"ERROR: Runner not found: {script_path}")
        return -1

    # Prefer venv Python
    if sys.platform == "win32":
        venv_python = ROOT_DIR / "river_env" / "Scripts" / "python.exe"
    else:
        venv_python = ROOT_DIR / "river_env" / "bin" / "python"

    python_exe = str(venv_python) if venv_python.exists() else sys.executable
    add_log(f"→ Python : {python_exe}")
    add_log(f"→ Script : {script_path.name}")

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    try:
        process = subprocess.Popen(
            [python_exe, str(script_path)],
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
                        add_log(f"{prefix}{text}")

        t_out = threading.Thread(target=read_stream, args=(process.stdout,),        daemon=True)
        t_err = threading.Thread(target=read_stream, args=(process.stderr, "[ERR] "), daemon=True)
        t_out.start()
        t_err.start()

        rc = await asyncio.get_event_loop().run_in_executor(None, process.wait)

        t_out.join(timeout=3.0)
        t_err.join(timeout=3.0)

        add_log(f"→ Process exited: code {rc}")
        return rc

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
    global active_process, active_task

    if active_task and not active_task.done():
        active_task.cancel()
        active_task = None

    if active_process:
        try:
            if active_process.poll() is None:
                active_process.kill()
        except Exception:
            pass
        active_process = None

    pipeline_state.update({
        "status":          "idle",
        "current_step":    0,
        "selected_rivers": [],
        "started_at":      None,
        "error_msg":       None,
    })
    pipeline_logs.clear()
    add_log("Pipeline reset. Ready to start.")
    return {"ok": True}


@app.post("/api/pipeline/step1")
async def start_step1(request: Request):
    global active_task

    if pipeline_state["status"] not in ("idle", "error"):
        raise HTTPException(409, f"Pipeline busy: {pipeline_state['status']}")

    body     = await request.json()
    selected = body.get("rivers", [])
    if not selected:
        raise HTTPException(400, "No rivers selected")

    cfg = load_config()
    cfg["selected_rivers"] = selected
    save_config(cfg)

    pipeline_state.update({
        "status":          "running_step1",
        "current_step":    1,
        "selected_rivers": selected,
        "started_at":      datetime.now().isoformat(),
        "error_msg":       None,
    })
    pipeline_logs.clear()

    preview = ', '.join(selected[:5]) + (f" +{len(selected)-5} more" if len(selected) > 5 else "")
    add_log(f"STEP 1 — GEE Export")
    add_log(f"  Rivers : {preview}")
    add_log(f"  Bands  : {cfg.get('selected_bands')}")
    add_log(f"  Target : {cfg.get('export_target', 'drive').upper()}")

    async def _run():
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
        log_event.set()

    active_task = asyncio.create_task(_run())
    return {"ok": True}


@app.post("/api/pipeline/step2")
async def start_step2():
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
    global active_task

    if pipeline_state["status"] not in ("done_step2", "error"):
        raise HTTPException(409, f"Cannot start Step 3 — status: {pipeline_state['status']}")

    pipeline_state["status"]       = "running_step3"
    pipeline_state["current_step"] = 3
    add_log("STEP 3 — Merge GeoTIFF Tiles")

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
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)