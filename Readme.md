# 🛰️ River Sentinel — Complete Technical Reference

> **Automated satellite imagery pipeline for Indian rivers.**
> Exports Sentinel-2 multispectral imagery + SRTM DEM from Google Earth Engine,
> downloads them from Google Drive, merges tile fragments, and cleans raster outputs —
> all orchestrated through an interactive web dashboard.

---

## Table of Contents

1. [What Does This System Do?](#1-what-does-this-system-do)
2. [Architecture Overview](#2-architecture-overview)
3. [Folder Structure](#3-folder-structure)
4. [How Data Flows Through the System](#4-how-data-flows-through-the-system)
5. [File-by-File Reference](#5-file-by-file-reference)
   - [main.py — FastAPI Backend](#mainpy--fastapi-backend)
   - [config.json — Shared Configuration](#configjson--shared-configuration)
   - [runners/run_step1.py — GEE Export Runner](#runnersrun_step1py--gee-export-runner)
   - [runners/run_step2.py — Drive Download Runner](#runnersrun_step2py--drive-download-runner)
   - [runners/run_step3.py — Merge Runner](#runnersrun_step3py--merge-runner)
   - [scripts/gee_export.py — GEE Export Logic](#scriptsgee_exportpy--gee-export-logic)
   - [scripts/drive_download.py — Google Drive Downloader](#scriptsdrive_downloadpy--google-drive-downloader)
   - [scripts/merge_tiles.py — Local Tile Merger](#scriptsmerge_tilespy--local-tile-merger)
   - [scripts/dem_clean.py — Raster Cleaner](#scriptsdem_cleanpy--raster-cleaner)
   - [scripts/vm_merge_gcs.py — Cloud VM Merger](#scriptsvm_merge_gcspy--cloud-vm-merger)
   - [terraform/main.tf — Cloud Infrastructure](#terraformmaintf--cloud-infrastructure)
   - [terraform/variables.tf — Terraform Variables](#terraformvariablestf--terraform-variables)
   - [templates/index.html — Dashboard UI](#templatesindexhtml--dashboard-ui)
6. [Configuration Reference (config.json)](#6-configuration-reference-configjson)
7. [API Endpoints Reference](#7-api-endpoints-reference)
8. [The 3-Step Pipeline In Detail](#8-the-3-step-pipeline-in-detail)
9. [Cloud VM Merge (Alternative to Step 3)](#9-cloud-vm-merge-alternative-to-step-3)
10. [Terraform Infrastructure](#10-terraform-infrastructure)
11. [DEM / Sentinel Cleaning Tool](#11-dem--sentinel-cleaning-tool)
12. [Key Design Decisions & Why](#12-key-design-decisions--why)
13. [Setup & First Run](#13-setup--first-run)
14. [Troubleshooting](#14-troubleshooting)

---

## 1. What Does This System Do?

River Sentinel collects and prepares satellite data for Indian rivers in three steps:

| Step | What Happens | Where |
|------|--------------|-------|
| **Step 1 — GEE Export** | Submits export tasks to Google Earth Engine (GEE). GEE creates cloud-free Sentinel-2 composites and SRTM DEMs for each river and saves them as GeoTIFFs. | Google's servers (async) |
| **Step 2 — Drive Download** | Downloads the completed GeoTIFF tiles from Google Drive to your local disk. Supports resume — interrupted downloads continue from where they left off. | Your machine ↔ Google Drive |
| **Step 3 — Merge Tiles** | GEE splits large rivers into numbered tiles (`-000.tif`, `-001.tif`, …). This step merges all tiles for each river into a single seamless GeoTIFF. | Your machine (local) |

**Bonus tools:**
- **DEM Clean** — Remove invalid pixels (NoData, NaN, zero/negative elevation) from any GeoTIFF.
- **Cloud VM Merge** — Run the merge job on a GCP Compute Engine VM instead of locally (for very large rivers that won't fit on a laptop).
- **Terraform** — Provision and tear down the GCP VM and IAM roles with one click.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                   Browser (localhost:8000)                   │
│              templates/index.html (Dashboard UI)            │
└────────────────────┬────────────────────────────────────────┘
                     │  HTTP REST API + Server-Sent Events
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    main.py (FastAPI Backend)                 │
│  • Serves dashboard HTML                                    │
│  • Manages pipeline state (idle/running/done/error)         │
│  • Streams live logs to browser via SSE                     │
│  • Launches runner scripts as subprocesses                  │
│  • Manages gcloud / terraform CLI calls                     │
└──────┬───────────────┬────────────────┬────────────────────┘
       │               │                │
       ▼               ▼                ▼
 runners/          scripts/         terraform/
 run_step1.py   gee_export.py       main.tf
 run_step2.py   drive_download.py   variables.tf
 run_step3.py   merge_tiles.py
                dem_clean.py
                vm_merge_gcs.py
```

```
EXTERNAL SERVICES:
  Google Earth Engine  ←── Step 1: submit tasks, GEE runs exports
  Google Drive         ←── Step 1: receives exported GeoTIFFs
  Google Drive         ──→ Step 2: downloads tiles to local disk
  Google Cloud Storage ←── Cloud VM merge: reads tiles, writes merged files
  GCP Compute Engine   ←── Terraform/VM launch: runs vm_merge_gcs.py
```

---

## 3. Folder Structure

```
river project aiq/
│
├── main.py                 ← FastAPI web server (the central coordinator)
├── config.json             ← All user settings (paths, dates, bands, etc.)
├── requirements.txt        ← Python dependencies
├── client_secret.json      ← Google Drive OAuth app credentials (keep private!)
├── drive_token.pickle      ← Cached Drive OAuth token (auto-created on first login)
│
├── templates/
│   └── index.html          ← Single-page dashboard UI (HTML + CSS + JS)
│
├── runners/                ← Bridge scripts: read config.json, inject into scripts/
│   ├── run_step1.py        ← Step 1 runner: config inject → exec gee_export.py
│   ├── run_step2.py        ← Step 2 runner: config inject → import drive_download.py
│   └── run_step3.py        ← Step 3 runner: config inject → exec merge_tiles.py
│
├── scripts/                ← Actual processing logic
│   ├── gee_export.py       ← Submits GEE export tasks for all selected rivers
│   ├── drive_download.py   ← Downloads files from Google Drive (with resume)
│   ├── merge_tiles.py      ← Merges GeoTIFF tiles locally (chunked windowed I/O)
│   ├── dem_clean.py        ← Cleans DEM + Sentinel rasters (remove invalid pixels)
│   └── vm_merge_gcs.py     ← Merge script that runs inside GCP VM (reads/writes GCS)
│
├── terraform/              ← Cloud infrastructure-as-code
│   ├── main.tf             ← VM + IAM resource definitions
│   ├── variables.tf        ← Variable declarations with descriptions
│   ├── terraform.tfvars    ← Your actual values (GITIGNORED — never commit this)
│   ├── terraform.tfvars.example  ← Template for terraform.tfvars
│   └── .terraform/         ← Provider plugins (auto-downloaded by terraform init)
│
├── Imagery_Output/         ← Local output folder (created at runtime)
│   ├── Sentinel/           ← Downloaded tile fragments (*_sentinel-000.tif, etc.)
│   ├── DEM/                ← Downloaded DEM tiles
│   └── Sentinel_Merged/    ← Merged outputs (*_merged.tif), ready for QGIS
│
└── river_env/              ← Python virtual environment (create with venv)
```

---

## 4. How Data Flows Through the System

### Complete end-to-end flow:

```
User clicks "Start Export" in browser
          │
          ▼
POST /api/pipeline/step1 (main.py)
  • Writes selected_rivers to config.json
  • Sets pipeline_state["status"] = "running_step1"
  • asyncio.create_task() → background task
          │
          ▼ (background)
subprocess Popen([python, "runners/run_step1.py"])
  • load_cfg() reads config.json
  • Reads gee_export.py source as text
  • Injects config overrides (SHAPEFILE_PATH, SPECIFIC_RIVERS, etc.)
  • exec(compile(patched_source)) → runs gee_export.main()
          │
          ▼
gee_export.main()
  • gpd.read_file(shapefile) → river line geometries
  • create_buffered_geometries() → 10km polygons around each river
  • ee.Initialize() → connect to GEE API
  • For each river:
    • export_sentinel_to_drive() → GEE task submitted (returns immediately)
    • export_dem_to_drive()      → GEE task submitted
  • Task IDs logged — actual export runs on Google's servers
          │
          ▼ (USER WAITS – monitor at code.earthengine.google.com/tasks)
          │ GEE exports GeoTIFFs to Google Drive
          ▼
User clicks "Start Download"
          │
          ▼
POST /api/pipeline/step2 (main.py)
  • subprocess Popen([python, "runners/run_step2.py"])
          │
          ▼
run_step2.py
  • importlib.util → loads drive_download.py as module
  • Overrides module globals (TOKEN_FILE, SENTINEL_LOCAL, etc.)
  • mod.main() → downloads all tiles
          │
          ▼
drive_download.main()
  • get_creds() → OAuth2 token from drive_token.pickle or browser login
  • find_folder_id() → Drive API: get folder ID from name
  • list_files_in_folder() → Drive API: list all .tif files
  • download_file() → streaming HTTP download with RESUME support
  • Files saved to: Imagery_Output/Sentinel/*.tif + Imagery_Output/DEM/*.tif
          │
          ▼
User clicks "Start Merge"
          │
          ▼
POST /api/pipeline/step3 (main.py)
  • subprocess Popen([python, "runners/run_step3.py"])
          │
          ▼
run_step3.py
  • Reads source of merge_tiles.py
  • Regex-patches input_folder and output_folder assignments
  • exec(compile(patched_source)) → runs merge_tiles module-level code
          │
          ▼
merge_tiles.py (exec'd)
  • glob.glob("Imagery_Output/Sentinel/*.tif") → find all tiles
  • Group tiles by river name (Ganga_sentinel-000, -001, -002 → one group)
  • For each river: merge_tiled() → chunked windowed I/O merge
  • Output: Imagery_Output/Sentinel_Merged/Ganga_sentinel_merged.tif
          │
          ▼
✅ DONE — merged files ready for QGIS / analysis
```

### Live log streaming to browser (Server-Sent Events):

```
add_log("some message")          ← called anywhere in main.py
  • Appends {"ts": "14:32:07", "msg": "some message"} to pipeline_logs
  • Calls log_event.set()         ← wakes up all SSE generators
          │
          ▼
GET /api/logs/stream (already open connection in browser)
  event_generator() was sleeping at:
    await asyncio.wait_for(log_event.wait(), timeout=1.0)
  Now wakes up → sends new log entries → sleeps again
          │
          ▼
Browser EventSource receives:
  data: {"type": "log", "index": 5, "ts": "14:32:07", "msg": "some message"}
          │
          ▼
JavaScript appends line to the log panel in index.html
```

---

## 5. File-by-File Reference

---

### `main.py` — FastAPI Backend

**Role:** The central coordinator. Serves the web UI, handles all API requests, manages the pipeline state machine, and launches child processes.

#### Key Global Variables

| Variable | Type | Purpose |
|----------|------|---------|
| `pipeline_state` | `dict` | Single source of truth for pipeline status. Keys: `status`, `current_step`, `selected_rivers`, `started_at`, `error_msg`. |
| `pipeline_logs` | `list[dict]` | All log lines for the current run. Each entry: `{"ts": "HH:MM:SS", "msg": "..."}`. Cleared on each new run. |
| `log_event` | `asyncio.Event` | Set by `add_log()` to wake sleeping SSE stream generators. |
| `active_process` | `subprocess.Popen` | Reference to the currently running child process. Used by reset to kill it. |
| `active_task` | `asyncio.Task` | Reference to the currently running async background task. |
| `dem_clean_state` | `dict` | Separate state for DEM clean operations (status, error_msg). |
| `terraform_state` | `dict` | Separate state for Terraform operations (status, last_log, error_msg). |
| `VM_CONFIG` | `dict` | Hardcoded GCP VM parameters (project_id, zone, machine_type, etc.). |
| `TERRAFORM_DIR` | `Path` | Absolute path to the `terraform/` folder. |
| `ROOT_DIR` | `Path` | Absolute path to the project root (where `main.py` lives). |

#### Key Functions

| Function | Why It Exists |
|----------|---------------|
| `_strip_ansi(text)` | Terraform outputs ANSI color codes. The browser terminal can't render them — they show as garbage characters. This regex removes them. |
| `load_config()` | Reads `config.json` and fills missing keys from `DEFAULT_CONFIG`. Ensures all code always gets a complete config dict. |
| `save_config(cfg)` | Writes config to `config.json` as pretty JSON. Called when user changes settings or when Step 1 saves selected rivers. |
| `add_log(message)` | Central logging function. Appends to `pipeline_logs` AND wakes SSE clients via `log_event.set()`. |
| `_run_runner(script_name)` | Launches a `runner/*.py` script as a child process. Reads stdout/stderr in background threads (to avoid blocking the async loop). Returns exit code. |
| `lifespan(app)` | FastAPI startup hook. Creates `log_event` after the event loop starts (can't create asyncio.Event before the loop). |
| `stream_logs()` | Server-Sent Events endpoint. Keeps the HTTP connection open and pushes new logs as they arrive. Sends heartbeats every second. |
| `_run_terraform(cmd_args)` | Runs `terraform init` then the specified command. Streams output to logs. Strips ANSI codes. |

#### State Machine

```
idle
  │  POST /api/pipeline/step1
  ▼
running_step1
  │  Step 1 success
  ▼
done_step1 ─────── POST /api/pipeline/step2 ──► running_step2
                                                      │ success
                                                      ▼
                                                 done_step2
                                                      │ POST /api/pipeline/step3
                                                      ▼
                                                 running_step3
                                                      │ success
                                                      ▼
                                                     done

Any step failure → error (error_msg set)
POST /api/pipeline/reset → idle (from any state)
```

---

### `config.json` — Shared Configuration

**Role:** The single file that bridges `main.py` (the web server) and the `runners/*.py` scripts (subprocesses). Because subprocesses don't share memory with the server, all settings are persisted here.

**Written by:**
- `main.py /api/config POST` — when user saves settings from the UI
- `main.py /api/pipeline/step1` — to save the selected_rivers list before launching run_step1.py

**Read by:**
- `main.py /api/config GET` — to populate the UI settings form
- `main.py /api/rivers GET` — to get the shapefile path
- `runners/run_step1.py` — reads all export settings
- `runners/run_step2.py` — reads download paths and credentials
- `runners/run_step3.py` — reads output folder path

| Key | Type | What It Controls |
|-----|------|-----------------|
| `shapefile_path` | string | Path to `River_India_Final.shp`. If set, river list comes from shapefile. If empty, uses the built-in list of ~120 rivers. |
| `output_base_folder` | string | Root local folder for all downloaded imagery (e.g. `C:/Users/.../Imagery_Output`). |
| `sentinel_subfolder` | string | Sub-folder name for Sentinel tiles (default: `"Sentinel"`). |
| `dem_subfolder` | string | Sub-folder name for DEM tiles (default: `"DEM"`). |
| `credentials_file` | string | Path to `client_secret.json` (OAuth2 Desktop App credentials from GCP Console). |
| `token_file` | string | Path to `drive_token.pickle` (cached OAuth token). Auto-created on first Drive login. |
| `drive_folder` | string | Root Google Drive folder (default: `"River_Imagery_Batch"`). |
| `drive_sentinel_folder` | string | Drive folder for Sentinel tiles (default: `"River_Imagery_Batch/Sentinel"`). |
| `drive_dem_folder` | string | Drive folder for DEM tiles (default: `"River_Imagery_Batch/DEM"`). |
| `gee_project` | string | Google Earth Engine Cloud Project ID. Must have EE API enabled. |
| `export_target` | string | `"drive"` / `"gcs"` / `"both"`. Determines where GEE saves exports. |
| `selected_bands` | list | Sentinel-2 bands to export. E.g. `["B2","B3","B4"]` for RGB only. |
| `gcs_bucket` | string | GCS bucket name (default: `"aiq-river-imagery"`). |
| `buffer_distance` | int | River buffer in metres (default: `10000` = 10 km each side). |
| `resolution` | int | Export pixel size in metres (default: `10` = native Sentinel-2 resolution). |
| `max_cloud_cover` | int | Max cloud % per scene (default: `10`). Higher values include more images but with more clouds. |
| `start_date` / `end_date` | string | Date range for imagery search (format: `"YYYY-MM-DD"`). |
| `skip_existing` | bool | If `true`, rivers with existing local files are skipped in Step 1. |
| `selected_rivers` | list | **Written by Step 1 endpoint. Read by run_step1.py.** The rivers to process in the current run. |

---

### `runners/run_step1.py` — GEE Export Runner

**Role:** Bridge between `main.py` and `scripts/gee_export.py`.

**Why it exists:** `gee_export.py` uses module-level configuration variables. The runner injects the correct values from `config.json` by patching the source code before executing it.

**Key technique — source code injection:**
```python
# 1. Read gee_export.py as text
with open(SCRIPT) as f:
    source = f.read()

# 2. Build override block
overrides = "SHAPEFILE_PATH = '/path/to/shapefile.shp'\nSPECIFIC_RIVERS = ['Ganga']\n..."

# 3. Insert before first 'def' or 'class'
modified = '\n'.join(lines[:insert_at]) + '\n' + overrides + '\n'.join(lines[insert_at:])

# 4. Compile and execute
exec(compile(modified, str(SCRIPT), "exec"), {'__name__': '__main__', ...})
```

**Why exec() instead of import?**
> Importing `gee_export` would use the original file from disk — the overrides wouldn't apply. `exec()` runs the MODIFIED source, so the overrides ARE in effect when `main()` runs.

**Data passed from config.json → gee_export.py:**

| config.json key | gee_export.py variable |
|-----------------|------------------------|
| `shapefile_path` | `SHAPEFILE_PATH` |
| `selected_rivers` | `SPECIFIC_RIVERS` |
| `output_base_folder` | `OUTPUT_BASE_FOLDER` |
| `buffer_distance` | `BUFFER_DISTANCE` |
| `resolution` | `RESOLUTION` |
| `start_date` | `START_DATE` |
| `end_date` | `END_DATE` |
| `max_cloud_cover` | `MAX_CLOUD_COVER` |
| `selected_bands` | `SELECTED_BANDS` |
| `export_target` | `EXPORT_TARGET` |
| `gee_project` | `GEE_PROJECT` |

---

### `runners/run_step2.py` — Drive Download Runner

**Role:** Bridge between `main.py` and `scripts/drive_download.py`.

**Key technique — module-level global override:**
```python
# Load drive_download.py as a Python module
spec = importlib.util.spec_from_file_location("drive_download", SCRIPT)
mod  = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

# Override globals in the loaded module
mod.TOKEN_FILE       = cfg.get("token_file", "")
mod.CREDENTIALS_FILE = cfg.get("credentials_file", "")
mod.SENTINEL_LOCAL   = os.path.join(output_base, sentinel_sub)

# Run drive_download's main function with overridden globals
mod.main()
```

**Why importlib instead of exec() like Step 1?**
> `drive_download.py` wraps ALL its logic in `main()`. This means importing it doesn't trigger any I/O — it just defines functions. We can safely `exec_module()` it and then override globals before calling `mod.main()`. `gee_export.py` by contrast calls `main()` at the bottom, so exec() is needed to inject before those calls run.

---

### `runners/run_step3.py` — Merge Runner

**Role:** Bridge between `main.py` and `scripts/merge_tiles.py`.

**Key technique — regex path patching:**
```python
# Read merge_tiles.py source
with open(SCRIPT) as f:
    source = f.read()

# Replace the empty variable assignments with real paths
patched = re.sub(r'input_folder\s*=\s*r?"[^"]*"',
    lambda _: f'input_folder  = r"{input_folder}"', source)
patched = re.sub(r'output_folder\s*=\s*r?"[^"]*"',
    lambda _: f'output_folder = r"{output_folder}"', patched)

# Execute patched source
exec(compile(patched, str(SCRIPT), "exec"), {"__name__": "__main__"})
```

**Why regex instead of importlib?**
> `merge_tiles.py` runs its merge loop at MODULE LEVEL (not inside a function). There's no `main()` to call. The module-level code runs as soon as `exec_module()` is called. We need to patch the path variables BEFORE that code runs — regex substitution achieves this.

**Why `r"..."` prefix for Windows paths?**
> Windows paths use backslashes (`C:\Users\...`). In a regular Python string, `\U` would be interpreted as a Unicode escape. The raw string prefix `r"..."` tells Python to treat backslashes literally.

---

### `scripts/gee_export.py` — GEE Export Logic

**Role:** Submits satellite imagery export tasks to Google Earth Engine.

#### Key Concepts

**Sentinel-2 Median Composite:**
```python
collection = (
    ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
    .filterBounds(ee_geometry)      # Only scenes covering this river
    .filterDate(START_DATE, END_DATE)
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', MAX_CLOUD_COVER))
)
image = collection.median()         # Per-pixel median → removes clouds
        .select(SELECTED_BANDS)     # Keep only needed bands
        .clip(ee_geometry)          # Crop to river polygon
```

**Why median?**
> A cloud pixel is bright (high reflectance). The median of 10 images taken at the same location over a year usually picks a cloud-free observation because most days are clear. The median is more robust than the mean (which would be biased high by cloud pixels).

**River buffer:**
```python
gdf_proj = gdf.to_crs("EPSG:3857")   # Project to metres
gdf_buffered['geometry'] = gdf_proj.geometry.buffer(10000)  # 10 km buffer
gdf_buffered = gdf_buffered.to_crs("EPSG:4326")  # Back to lat/lon for GEE
```
> We buffer in EPSG:3857 (Web Mercator) so the distance is in metres, not degrees. A degree of longitude narrows near the poles — buffering in degrees would give inconsistent widths.

**Export destinations:**
| `export_target` | What Happens |
|-----------------|-------------|
| `"drive"` | Files → Google Drive (for Step 2 download to local disk) |
| `"gcs"` | Files → GCS bucket (for cloud VM merge) |
| `"both"` | Files → both Drive AND GCS simultaneously |

---

### `scripts/drive_download.py` — Google Drive Downloader

**Role:** Downloads GeoTIFF tiles from Google Drive to local disk with resume support.

#### Authentication Flow

```
First run:
  drive_token.pickle missing
    → InstalledAppFlow.from_client_secrets_file(client_secret.json)
    → Opens browser → User grants permission
    → Token saved to drive_token.pickle

Subsequent runs:
  drive_token.pickle exists
    → pickle.load() → credentials object
    → If expired: creds.refresh(Request()) → token refreshed silently
    → If valid: use as-is
```

#### Resume Logic

```python
local_size = os.path.getsize(local_path)  # How much we already have

if local_size == drive_size:              # Already complete → skip
    return "skipped"
elif 0 < local_size < drive_size:        # Partial → resume
    headers["Range"] = f"bytes={local_size}-"  # HTTP Range request
    mode = 'ab'  # Append binary
else:                                     # Fresh download
    mode = 'wb'  # Write binary
```

> **Why HTTP Range?** The Drive API supports RFC 7233 Range Requests. Setting `Range: bytes=N-` tells Google's server to skip the first N bytes and send the rest. Combined with `'ab'` (append) file mode, this seamlessly continues interrupted downloads.

---

### `scripts/merge_tiles.py` — Local Tile Merger

**Role:** Merges GeoTIFF tile fragments (from GEE's tiled export) into one file per river.

#### Why Chunked Windowed I/O?

A single large river (e.g., Ganga) might produce 10–20 tiles totalling 20+ GB. Loading everything into RAM at once would require 64+ GB. Instead:

```
For each 2048-row horizontal strip:
    canvas = np.full((bands, 2048, width), fill_value=-9999)  # Empty canvas
    mask   = np.zeros((2048, width), dtype=bool)               # Track filled pixels

    For each tile that overlaps this strip:
        win = compute intersection window
        data = tile.read(window=win)
        canvas[not yet filled] = data   # First-tile-wins
        mask[intersection] = True

    output_file.write(canvas, window=current_strip)   # Write strip to disk
```

**Memory usage:** `~2048 × total_width × n_bands × 4 bytes`
For a 50,000-pixel wide river with 10 bands: ≈ 4 GB per chunk (fits in 16 GB RAM)
Reduce `CHUNK` to 1024 or 512 if you have less RAM.

#### Tile Grouping

```
Input files:        Output group:
Ganga_sentinel-000.tif  ┐
Ganga_sentinel-001.tif  ├─→ "Ganga_sentinel" → Ganga_sentinel_merged.tif
Ganga_sentinel-002.tif  ┘
Yamuna_sentinel.tif     ──→ "Yamuna_sentinel" → Yamuna_sentinel_merged.tif
```

Grouping rule: `filename.split('-000')[0]` if `-000` is in the name, else strip `.tif`.

#### Output Settings

| Setting | Value | Why |
|---------|-------|-----|
| `dtype` | `float32` | Covers both Sentinel reflectance (0–10000) and DEM elevation (any real number) |
| `compress` | `lzw` | Lossless compression. Typically 50–70% size reduction. |
| `tiled` + `blockxsize/y` | `512×512` | Internal tiles allow fast random-access reads in QGIS without loading the whole file |
| `nodata` | `-9999` | QGIS renders `-9999` pixels as transparent. Safer than `0` (water pixels can be 0) |
| `BIGTIFF` | `YES` | Standard GeoTIFF has a 4 GB limit. BIGTIFF removes this limit. |

---

### `scripts/dem_clean.py` — Raster Cleaner

**Role:** Remove invalid pixels from a GeoTIFF (DEM or Sentinel-2 multi-band).

#### Auto-Detection Logic

```python
n_bands = src.count
MODE = "sentinel" if n_bands > 1 else "dem"
```

> Single band = DEM (SRTM has only one elevation band)
> Multiple bands = Sentinel-2 (10 spectral bands)

#### Mask Construction

**DEM mode:**
```python
mask_nan    = np.isnan(band)              # NaN from float conversion
mask_nodata = (band == nodata_in)         # Declared NoData value in file
mask_zero   = (band <= 0) & ~mask_nan    # Zero/negative elevation = invalid

combined = mask_nan | mask_nodata | mask_zero
band[combined] = -9999.0                  # Replace all invalid with standardised NoData
```

**Sentinel mode:**
```python
mask_nan     = np.any(np.isnan(chunk), axis=0)     # NaN in ANY band
mask_nodata  = np.all(chunk == nodata_in, axis=0)  # ALL bands = declared NoData
mask_allzero = np.all(chunk == 0, axis=0)           # ALL bands = 0 (GEE padding)

combined = mask_nan | mask_nodata | mask_allzero
chunk[:, combined] = -9999.0                         # Replace across all bands
```

> **Why `np.all()` for nodata/zero in Sentinel?** A single band being zero is plausible (some spectral bands can legitimately be zero). But ALL 10 bands being zero or exactly matching NoData simultaneously is almost certainly padding, not real data.

#### Overview Pyramids

```python
with rasterio.open(output, "r+") as dst:
    dst.build_overviews([2, 4, 8, 16], Resampling.average)
```

> Overview levels [2, 4, 8, 16] create pre-computed 1/2, 1/4, 1/8, 1/16 resolution versions. QGIS reads the appropriate level based on zoom — massive speed improvement for large files.

---

### `scripts/vm_merge_gcs.py` — Cloud VM Merger

**Role:** Merge script that runs INSIDE a GCP Compute Engine VM. Reads tiles from GCS, merges them, uploads merged files back to GCS.

**Key difference from merge_tiles.py:**
- Uses `gsutil` CLI for all GCS I/O (not the Python library) to avoid SSL conflicts
- Downloads tiles to `/tmp/river_merge/input/` (VM local disk)
- Uploads merged files then immediately deletes temp files (save disk space)
- Runs in a cloud VM with 200 GB SSD and 32 GB RAM

**Idempotent skip:**
```python
def already_merged(bucket, output_prefix, river_name):
    r = gsutil("-q", "stat", f"gs://{bucket}/{output_prefix}/{river_name}_merged.tif")
    return r.returncode == 0   # 0 = file exists = already merged
```
> Safe to restart the VM script — already-merged rivers are skipped.

---

### `terraform/main.tf` — Cloud Infrastructure

**Role:** Defines the GCP infrastructure as code.

#### What Terraform Creates

```
1. google_storage_bucket_iam_member  →  VM service account can read/write GCS bucket
2. google_project_iam_member         →  VM service account can delete itself
3. google_compute_instance           →  The merge VM (Debian 12, 4 CPU, 32 GB RAM, 200 GB SSD)
```

#### Startup Script Flow

The startup script in `locals.startup_script` runs as root on first VM boot:

```
[1/4] apt-get install + pip install rasterio numpy
[2/4] gsutil cp gs://<bucket>/scripts/vm_merge_gcs.py /tmp/river_merge/
[3/4] python3 vm_merge_gcs.py --bucket ... --input-prefix ... --output-prefix ...
[4/4] gcloud compute instances delete $INSTANCE --zone=$ZONE --quiet
```

#### VM Self-Delete

The VM deletes itself when done because:
- Idle VMs still cost money
- The merge is a one-shot job
- `roles/compute.instanceAdmin.v1` IAM role (provisioned by Terraform) gives permission

#### Spot vs Standard

| Setting | Cost | Risk |
|---------|------|------|
| `use_spot = true` | ~60% cheaper | GCP can PREEMPT (kill) during merge |
| `use_spot = false` (default) | Full price | Guaranteed to run to completion |

#### Lifecycle Ignore

```hcl
lifecycle {
  ignore_changes = [metadata]
}
```
> After the VM self-deletes, Terraform's state still has the old metadata recorded. Without `ignore_changes`, the next `terraform apply` would fail trying to modify a non-existent VM. With `ignore_changes`, Terraform simply creates a fresh VM.

---

### `terraform/variables.tf` — Terraform Variables

All variables used in `main.tf` are declared here with their types, descriptions, and defaults.

| Variable | Required in tfvars? | Default | Why |
|----------|---------------------|---------|-----|
| `project_id` | Optional | `"plucky-sight-423703-k5"` | Your GCP project ID |
| `project_number` | **YES — no default** | (none) | Needed for service account email |
| `region` | Optional | `"us-central1"` | GCP region |
| `zone` | Optional | `"us-central1-a"` | Compute Engine zone |
| `bucket_name` | Optional | `"aiq-river-imagery"` | GCS bucket with tiles |
| `input_prefix` | Optional | `"Sentinel"` | GCS folder for raw tiles |
| `output_prefix` | Optional | `"Sentinel_Merged"` | GCS folder for merged output |
| `vm_name` | Optional | `"river-merge-vm"` | Compute Engine instance name |
| `machine_type` | Optional | `"n2-highmem-4"` | 4 CPU, 32 GB RAM |
| `disk_size_gb` | Optional | `200` | VM disk size in GB |
| `use_spot` | Optional | `false` | Spot VM (cheaper but preemptible) |
| `selected_rivers` | Optional | `[]` | Rivers to merge (empty = all) |

---

### `templates/index.html` — Dashboard UI

Single-page HTML/CSS/JS dashboard served by `main.py`. Communicates with the backend exclusively via:
- **REST API calls** (`fetch()`) — for actions (start step, save config, etc.)
- **Server-Sent Events** (`new EventSource('/api/logs/stream')`) — for live log streaming

---

## 6. Configuration Reference (config.json)

Copy this template and fill in your paths:

```json
{
  "shapefile_path":       "C:/Users/YourName/Downloads/River_India_Final.shp",
  "output_base_folder":   "C:/Users/YourName/Documents/Imagery_Output",
  "sentinel_subfolder":   "Sentinel",
  "dem_subfolder":        "DEM",
  "credentials_file":     "C:/Users/YourName/Documents/river project aiq/client_secret.json",
  "token_file":           "C:/Users/YourName/Documents/river project aiq/drive_token.pickle",
  "drive_folder":         "River_Imagery_Batch",
  "drive_sentinel_folder": "River_Imagery_Batch/Sentinel",
  "drive_dem_folder":     "River_Imagery_Batch/DEM",
  "gee_project":          "your-gcp-project-id",
  "export_target":        "drive",
  "selected_bands":       ["B2", "B3", "B4", "B8"],
  "gcs_bucket":           "your-gcs-bucket-name",
  "buffer_distance":      10000,
  "resolution":           10,
  "max_cloud_cover":      10,
  "start_date":           "2025-01-01",
  "end_date":             "2025-12-31",
  "skip_existing":        true,
  "selected_rivers":      []
}
```

---

## 7. API Endpoints Reference

### Pipeline

| Method | Path | Body | Description |
|--------|------|------|-------------|
| GET | `/` | — | Serve dashboard HTML |
| GET | `/api/rivers` | — | List available rivers (from shapefile or fallback list) |
| GET | `/api/config` | — | Get current config settings |
| POST | `/api/config` | `{key: value, ...}` | Update and save config |
| GET | `/api/status` | — | Get pipeline state + log count |
| GET | `/api/logs?since=N` | — | Get log lines from index N |
| GET | `/api/logs/stream` | — | SSE stream: live logs + status updates |
| POST | `/api/pipeline/step1` | `{"rivers": ["Ganga", ...]}` | Start Step 1 (GEE Export) |
| POST | `/api/pipeline/step2` | — | Start Step 2 (Drive Download) |
| POST | `/api/pipeline/step3` | — | Start Step 3 (Merge Tiles) |
| POST | `/api/pipeline/reset` | — | Kill running process, reset to idle |

### Cloud VM

| Method | Path | Body | Description |
|--------|------|------|-------------|
| POST | `/api/vm/launch` | `{"rivers": [...]}` | Launch GCP merge VM |
| GET | `/api/vm/status` | — | Check if VM is still running |
| DELETE | `/api/vm/kill` | — | Force-delete the VM |

### DEM Clean

| Method | Path | Body | Description |
|--------|------|------|-------------|
| POST | `/api/dem-clean` | `{"input_path": "...", "output_path": "..."}` | Start DEM/Sentinel cleaning |
| GET | `/api/dem-clean/status` | — | Get DEM clean state + log count |
| GET | `/api/dem-clean/logs?since=N` | — | Get DEM clean log lines |
| POST | `/api/dem-clean/reset` | — | Stop DEM clean, reset to idle |

### Terraform

| Method | Path | Body | Description |
|--------|------|------|-------------|
| POST | `/api/terraform/apply` | — | Run `terraform apply -auto-approve` |
| POST | `/api/terraform/destroy` | — | Run `terraform destroy -auto-approve` |
| GET | `/api/terraform/status` | — | Get Terraform state + log count |
| GET | `/api/terraform/logs?since=N` | — | Get Terraform log lines |

---

## 8. The 3-Step Pipeline In Detail

### Step 1 — GEE Export

> GEE submits tasks (returns immediately). The ACTUAL export runs on Google's servers for 5–60 minutes per river.

1. The selected rivers and all configuration are saved to `config.json`
2. `run_step1.py` is launched as a subprocess
3. It patches and executes `gee_export.py`
4. For each river:
   - The river geometry is read from the shapefile
   - A 10 km buffer polygon is created
   - A Sentinel-2 cloud-free median composite is computed by GEE
   - Export task submitted to GEE (call returns in ~1 second; GEE processes in background)
   - A DEM export task is also submitted
5. When all tasks are submitted, Step 1 is complete

**After Step 1:** Monitor at [code.earthengine.google.com/tasks](https://code.earthengine.google.com/tasks)
Wait until all tasks show **COMPLETED** before starting Step 2.

---

### Step 2 — Drive Download

1. `run_step2.py` is launched as a subprocess
2. It loads `drive_download.py` as a module and overrides path globals
3. `drive_download.main()` authenticates with Google Drive (OAuth2)
4. Lists all `.tif` files in `River_Imagery_Batch/Sentinel/` and `River_Imagery_Batch/DEM/`
5. Downloads each file:
   - If already fully downloaded → skip
   - If partially downloaded → resume via HTTP Range header
   - If not downloaded → fresh download

**Files saved to:**
```
Imagery_Output/
├── Sentinel/
│   ├── Ganga_sentinel-000.tif
│   ├── Ganga_sentinel-001.tif
│   └── Yamuna_sentinel.tif
└── DEM/
    ├── Ganga_dem.tif
    └── Yamuna_dem.tif
```

---

### Step 3 — Merge Tiles

1. `run_step3.py` is launched as a subprocess
2. It reads `merge_tiles.py` source, patches `input_folder` and `output_folder`
3. Executes the patched source
4. `merge_tiles.py` groups all tiles by river name
5. For each river: calls `merge_tiled()` with chunked windowed I/O
6. Outputs one merged file per river

**Files written to:**
```
Imagery_Output/
└── Sentinel_Merged/
    ├── Ganga_sentinel_merged.tif    ← ready for QGIS
    └── Yamuna_sentinel_merged.tif
```

---

## 9. Cloud VM Merge (Alternative to Step 3)

Use this when:
- Rivers are too large for local RAM/disk
- You want faster processing (GCP internal network is ~10× faster than home internet)
- You want to merge all rivers in a batch without tying up your machine

### Two Ways to Launch

**Via Dashboard (Terraform tab):**
1. Fill in `terraform.tfvars`
2. Click "Apply" in the Terraform panel → VM is created
3. Monitor with the output command printed after apply

**Via Dashboard (VM Launch button):**
1. Click "Launch Cloud Merge VM"
2. `main.py` builds a startup script and calls `gcloud compute instances create`
3. VM boots, merges all tiles, uploads merged files to GCS, self-deletes

### Monitor VM logs

```bash
gcloud compute instances get-serial-port-output river-merge-vm \
  --zone=us-central1-a --project=plucky-sight-423703-k5
```

### Download merged files from GCS

```bash
gsutil -m cp "gs://aiq-river-imagery/Sentinel_Merged/*_merged.tif" ./merged_files/
```

---

## 10. Terraform Infrastructure

### One-time setup

1. Copy `terraform.tfvars.example` → `terraform.tfvars`
2. Fill in `project_id`, `project_number`
3. Upload `vm_merge_gcs.py` to GCS:
   ```bash
   gsutil cp scripts/vm_merge_gcs.py gs://aiq-river-imagery/scripts/vm_merge_gcs.py
   ```

### Via Dashboard

- **Apply** — Creates VM + IAM roles, VM starts running immediately
- **Destroy** — Removes IAM roles (VM is already self-deleted)

### Via CLI

```bash
cd terraform
terraform init      # Download Google provider plugin
terraform apply     # Create VM + IAM
terraform destroy   # Remove IAM (VM already gone after merge)
```

---

## 11. DEM / Sentinel Cleaning Tool

### From Dashboard

1. Go to the **DEM Clean** tab
2. Enter input path (any GeoTIFF — DEM or Sentinel)
3. Enter output path
4. Click Run

### From Command Line

```bash
# Auto-detect type
python scripts/dem_clean.py input.tif output_clean.tif

# Force DEM mode
python scripts/dem_clean.py input.tif output_clean.tif --type dem

# Force Sentinel mode
python scripts/dem_clean.py input.tif output_clean.tif --type sentinel

# Use smaller chunks (less RAM, slower)
python scripts/dem_clean.py input.tif output_clean.tif --chunk 256
```

### What Gets Removed

| Pixel Type | DEM Mode | Sentinel Mode |
|------------|----------|---------------|
| NaN pixels | ✅ Removed | ✅ Removed (any band has NaN) |
| Declared NoData | ✅ Removed | ✅ Removed (ALL bands match NoData) |
| Zero/negative elevation | ✅ Removed | — |
| All-band zero pixels | — | ✅ Removed (GEE bounding box padding) |

Invalid pixels are replaced with `-9999.0` (float32 output) or `0` (uint16 input).

---

## 12. Key Design Decisions & Why

### Why FastAPI + Python instead of Django/Flask?

- **Async-native**: Pipeline steps run as `asyncio.Task` background coroutines. FastAPI's async design means the server keeps handling requests (logs, status polls) while the pipeline runs in background.
- **Server-Sent Events**: FastAPI natively supports streaming responses with `StreamingResponse` — no WebSocket library needed.
- **AutoDoc**: FastAPI generates `/docs` Swagger UI automatically.

### Why config.json instead of environment variables or a database?

- Subprocess isolation: each `runners/*.py` script is a separate process. Config.json is the simplest shared state mechanism.
- Human editable: users can open `config.json` in a text editor and change settings.
- Persistent: survives server restarts.

### Why exec(compile(...)) instead of subprocess for runners?

- **Speed**: No new Python interpreter startup for each runner. The venv is already loaded.
- **Output capture**: `subprocess.PIPE` lets us stream stdout/stderr to the dashboard.
- Actually, `_run_runner()` in `main.py` DOES use subprocess.Popen — the runners execute in a CHILD process. It's the runners that use exec() internally to run the actual scripts. This two-level design:
  - `main.py` → subprocess → runner (new Python process) → exec (same Python process, different source injected)

### Why chunked windowed I/O for the merge?

- RAM efficiency: a naive approach (`rasterio.merge.merge()`) loads all tiles at once — easily 50+ GB for large rivers.
- Disk I/O efficiency: writing in strips matches the GeoTIFF's internal tiling layout.
- Correctness: `BIGTIFF=YES` supports files larger than 4 GB.

### Why first-tile-wins in the merge algorithm?

- Seam lines: if two tiles overlap and we average them, there can be colour/value discontinuities at the overlap edge.
- First-tile-wins is deterministic, fast (one `np.where` operation), and produces seamless results.

### Why OAuth2 for Drive instead of a service account?

- GEE exports to THE USER'S personal Google Drive (not a shared service account Drive).
- Service accounts have their own separate Drive space — they cannot see the user's files.
- OAuth2 acts as the user, giving access to the user's Drive files.

---

## 13. Setup & First Run

### 1. Python Environment

```bash
cd "river project aiq"
python -m venv river_env
river_env\Scripts\activate    # Windows
# source river_env/bin/activate  # Linux/Mac
pip install -r requirements.txt
```

### 2. Google Earth Engine Setup

1. Go to [earthengine.google.com](https://earthengine.google.com) → Sign up
2. Create a GCP project with EE API enabled
3. Run: `gcloud auth application-default login`
4. Update `config.json`: set `gee_project` to your project ID

### 3. Google Drive Setup

1. Go to [GCP Console](https://console.cloud.google.com) → APIs → OAuth consent screen → create Desktop App credentials
2. Download `client_secret.json`, save to project root
3. Update `config.json`: set `credentials_file` and `token_file` paths

### 4. Download Shapefile

1. Download Indian river shapefile (e.g., from WRIS or Natural Earth)
2. Update `config.json`: set `shapefile_path` to the `.shp` file path

### 5. Start the Dashboard

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Open browser: http://localhost:8000

### 6. Run the Pipeline

1. **Settings tab** — verify all paths are correct, save
2. **Pipeline tab** — select rivers, click "Start Export"
3. Wait for GEE tasks to complete (~15–45 min per river)
4. Click "Start Download" — wait for files to download
5. Click "Start Merge" — wait for tiles to merge
6. Open merged `.tif` files in QGIS from `Imagery_Output/Sentinel_Merged/`

---

## 14. Troubleshooting

### "No rivers selected"
→ Go to Settings, verify `shapefile_path` exists on disk, save, then try again. Or just use the dropdown — the fallback list has 120+ rivers.

### "Shapefile read error"
→ Make sure `geopandas` is installed: `pip install geopandas`. Verify the `.shp` file exists at the exact path in config.

### Step 1 fails: GEE authentication error
→ Run `gcloud auth application-default login` in terminal. Make sure the GCP project has Earth Engine API enabled.

### Step 2 fails: Folder not found in Drive
→ Verify GEE tasks actually completed in the [task monitor](https://code.earthengine.google.com/tasks). The Drive folder only exists if GEE finished exporting.

### Step 2: "drive_token.pickle" expired
→ Delete `drive_token.pickle` and run Step 2 again. It will open a browser login.

### Step 3: OOM (out of memory)
→ Edit `merge_tiles.py`: reduce `CHUNK = 2048` to `CHUNK = 1024` or `512`. Or use the cloud VM merge instead.

### Step 3: "proj: pj_obj_create: cannot find proj.db"
→ PostgreSQL's PROJ is conflicting with rasterio's PROJ. `run_step3.py` sets `PROJ_DATA` and `PROJ_LIB` to pyproj's data dir before importing rasterio. If it still fails, run: `pip install --upgrade pyproj rasterio`.

### Terraform: "project_number is required"
→ Fill in `terraform/terraform.tfvars` with your project number. Find it:
```bash
gcloud projects describe YOUR_PROJECT_ID --format='value(projectNumber)'
```

### VM launch fails: "gcloud: command not found"
→ Install [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) and run `gcloud auth login`.

### DEM Clean: "❌ File not found"
→ The input path must be an ABSOLUTE path (not relative). On Windows, use forward slashes: `C:/Users/...` or `C:\\Users\\...`.

---

*Generated by River Sentinel — Last updated: April 2026*
