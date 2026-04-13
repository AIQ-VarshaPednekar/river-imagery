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
import re
import sys
import tempfile
import shutil
import time
import subprocess
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

# Strip ANSI escape codes from subprocess output (Terraform, etc.)
_ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

def _strip_ansi(text: str) -> str:
    return _ANSI_ESCAPE.sub('', text)

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
    # GCS
    "gcs_bucket":           "aiq-river-imagery",
    "gcs_sentinel_prefix":  "Sentinel",
    "gcs_merged_prefix":    "Sentinel_Merged",
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

VM_CONFIG = {
    "project_id":    "plucky-sight-423703-k5",
    "zone":          "us-central1-a",          
    "vm_name":       "river-merge-vm",
    "machine_type":  "n2-highmem-4",           # 4 CPU / 32 GB RAM
    "disk_size_gb":  "200GB",
    "bucket_name":   "aiq-river-imagery",
    "input_prefix":  "Sentinel",
    "output_prefix": "Sentinel_Merged",
}

# =============================================================================
# ROUTES — Cloud VM Merge
# =============================================================================

@app.post("/api/vm/launch")
async def launch_merge_vm(request: Request):
    """
    Creates a GCP Compute VM that:
      1. Downloads tiles from GCS bucket (Sentinel/ prefix)
      2. Groups by river name, skips already-merged rivers
      3. Merges tiles with windowed chunked I/O (same as merge_tiles.py)
      4. Uploads merged GeoTIFFs back to GCS (Sentinel_Merged/ prefix)
      5. Self-deletes the VM when done

    Requires:  gcloud CLI installed + `gcloud auth application-default login` run once.
    """
    global active_task

    body            = await request.json()
    selected_rivers = body.get("rivers", [])

    # ── Guard: don't launch if VM already exists ─────────────────────────────
    try:
        check = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: subprocess.run(
                ["gcloud", "compute", "instances", "describe",
                 VM_CONFIG["vm_name"],
                 "--zone",    VM_CONFIG["zone"],
                 "--project", VM_CONFIG["project_id"],
                 "--format=value(status)"],
                capture_output=True, text=True, timeout=15
            )
        )
        if check.returncode == 0:
            status = check.stdout.strip()
            if status in ("RUNNING", "PROVISIONING", "STAGING"):
                raise HTTPException(409, f"VM already running (status: {status}). Wait for it to finish.")
    except HTTPException:
        raise
    except Exception:
        pass   # VM doesn't exist yet — proceed

    # ── Build the startup script ─────────────────────────────────────────────
    rivers_arg = ("--rivers " + " ".join(selected_rivers)) if selected_rivers else ""

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

    # ── Write startup script to temp file ────────────────────────────────────
    tmp_script = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.sh', delete=False, encoding='utf-8'
        ) as f:
            f.write(startup_script)
            tmp_script = f.name

        add_log("☁  Launching GCP merge VM...")
        add_log(f"   Machine  : {VM_CONFIG['machine_type']} (4 CPU / 32 GB RAM)")
        add_log(f"   Zone     : {VM_CONFIG['zone']}")
        add_log(f"   Input    : gs://{VM_CONFIG['bucket_name']}/{VM_CONFIG['input_prefix']}/")
        add_log(f"   Output   : gs://{VM_CONFIG['bucket_name']}/{VM_CONFIG['output_prefix']}/")
        if selected_rivers:
            add_log(f"   Rivers   : {', '.join(selected_rivers)}")
        else:
            add_log("   Rivers   : ALL rivers found in bucket")

        gcloud_cmd = [
            "gcloud", "compute", "instances", "create", VM_CONFIG["vm_name"],
            "--zone",               VM_CONFIG["zone"],
            "--project",            VM_CONFIG["project_id"],
            "--machine-type",       VM_CONFIG["machine_type"],
            "--image-family",       "debian-12",
            "--image-project",      "debian-cloud",
            "--boot-disk-size",     VM_CONFIG["disk_size_gb"],
            "--boot-disk-type",     "pd-ssd",
            "--scopes",             "storage-full,logging-write,compute-rw",
            "--metadata-from-file", f"startup-script={tmp_script}",
            "--format",             "json",
        ]

        async def _create_vm():
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
    """Poll this to check if the merge VM is still alive."""
    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: subprocess.run(
                ["gcloud", "compute", "instances", "describe",
                 VM_CONFIG["vm_name"],
                 "--zone",    VM_CONFIG["zone"],
                 "--project", VM_CONFIG["project_id"],
                 "--format=value(status)"],
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
        # Non-zero = VM not found = it self-deleted = merge done
        return {"exists": False, "status": "DELETED", "running": False}
    except Exception as e:
        return {"exists": False, "status": "UNKNOWN", "running": False, "error": str(e)}


# =============================================================================
# ROUTES — DEM Clean
# =============================================================================

dem_clean_state: dict = {
    "status": "idle",   # idle | running | done | error
    "error_msg": None,
}
dem_clean_logs: list[dict] = []
dem_clean_task: Optional[asyncio.Task] = None

@app.get("/api/dem-clean/status")
async def get_dem_clean_status():
    return {**dem_clean_state, "log_count": len(dem_clean_logs)}

@app.get("/api/dem-clean/logs")
async def get_dem_clean_logs(since: int = 0):
    return {"logs": dem_clean_logs[since:], "total": len(dem_clean_logs)}

@app.post("/api/dem-clean")
async def run_dem_clean(request: Request):
    global dem_clean_task

    if dem_clean_state["status"] == "running":
        raise HTTPException(409, "DEM Clean is already running.")

    body       = await request.json()
    input_path = body.get("input_path", "").strip()
    output_path = body.get("output_path", "").strip()

    if not input_path:
        raise HTTPException(400, "input_path is required.")
    if not output_path:
        raise HTTPException(400, "output_path is required.")
    if not os.path.exists(input_path):
        raise HTTPException(400, f"Input file not found: {input_path}")

    # Ensure output directory exists
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    script_path = ROOT_DIR / "scripts" / "dem_clean.py"
    if not script_path.exists():
        raise HTTPException(500, f"Script not found: {script_path}")

    if sys.platform == "win32":
        venv_python = ROOT_DIR / "river_env" / "Scripts" / "python.exe"
    else:
        venv_python = ROOT_DIR / "river_env" / "bin" / "python"
    python_exe = str(venv_python) if venv_python.exists() else sys.executable

    dem_clean_state.update({"status": "running", "error_msg": None})
    dem_clean_logs.clear()

    def _add_dem_log(msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        dem_clean_logs.append({"ts": ts, "msg": msg})
        # Also mirror to main pipeline log for console visibility
        add_log(msg)

    _add_dem_log("DEM CLEAN — Starting")
    _add_dem_log(f"  Input  : {input_path}")
    _add_dem_log(f"  Output : {output_path}")

    async def _run():
        global active_process
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        try:
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

            t_out = threading.Thread(target=read_stream, args=(process.stdout,), daemon=True)
            t_err = threading.Thread(target=read_stream, args=(process.stderr, "[ERR] "), daemon=True)
            t_out.start(); t_err.start()

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
            log_event.set()

    dem_clean_task = asyncio.create_task(_run())
    return {"ok": True}


@app.post("/api/dem-clean/reset")
async def reset_dem_clean():
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
    """Emergency: force-delete the merge VM."""
    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: subprocess.run(
                ["gcloud", "compute", "instances", "delete",
                 VM_CONFIG["vm_name"],
                 "--zone",    VM_CONFIG["zone"],
                 "--project", VM_CONFIG["project_id"],
                 "--quiet"],
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

TERRAFORM_DIR = ROOT_DIR / "terraform"

terraform_state: dict = {
    "status":    "idle",   # idle | running | done | destroyed | error
    "last_log":  "",
    "error_msg": None,
}
terraform_logs: list[dict] = []
terraform_task: Optional[asyncio.Task] = None


def _add_tf_log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    terraform_logs.append({"ts": ts, "msg": msg})
    terraform_state["last_log"] = msg
    add_log(msg)   # mirror to main console


@app.get("/api/terraform/status")
async def get_tf_status():
    return {**terraform_state, "log_count": len(terraform_logs)}


@app.get("/api/terraform/logs")
async def get_tf_logs(since: int = 0):
    return {"logs": terraform_logs[since:], "total": len(terraform_logs)}


async def _run_terraform(cmd_args: list[str], success_status: str) -> None:
    """Run `terraform <cmd_args>` as a subprocess, stream output to logs."""
    global active_process

    if not TERRAFORM_DIR.exists():
        _add_tf_log(f"ERROR: terraform/ directory not found at {TERRAFORM_DIR}")
        terraform_state["status"] = "error"
        terraform_state["error_msg"] = "terraform/ directory not found"
        log_event.set()
        return

    # Find terraform executable
    tf_exe = shutil.which("terraform") or "terraform"

    env = os.environ.copy()
    env["TF_IN_AUTOMATION"] = "1"       # suppresses interactive prompts
    env["TF_CLI_ARGS"]      = "-no-color"  # disable ANSI color codes
    env["PYTHONIOENCODING"] = "utf-8"

    async def _execute(args):
        _add_tf_log(f"→ Running: terraform {' '.join(args)}")
        process = subprocess.Popen(
            [tf_exe] + args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,   # merge stderr into stdout
            cwd=str(TERRAFORM_DIR),
            universal_newlines=False,
            env=env,
        )
        active_process = process

        def _read():
            for raw in iter(process.stdout.readline, b''):
                if raw:
                    text = raw.decode("utf-8", errors="replace").rstrip()
                    text = _strip_ansi(text)   # remove ANSI color codes
                    if text:
                        _add_tf_log(text)

        t = threading.Thread(target=_read, daemon=True)
        t.start()
        rc = await asyncio.get_event_loop().run_in_executor(None, process.wait)
        t.join(timeout=5.0)
        return rc

    try:
        # Always init first (safe to re-run, no-op if already done)
        rc_init = await _execute(["init", "-input=false"])
        if rc_init != 0:
            raise RuntimeError(f"terraform init failed (exit {rc_init})")

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
        log_event.set()


@app.post("/api/terraform/apply")
async def terraform_apply():
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)