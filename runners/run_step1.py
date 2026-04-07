"""
Step 1 Runner — GEE Export
===========================
Reads config.json, injects config overrides into gee_export.py, and executes it.
"""

import json
import sys
import os
import re
from pathlib import Path

os.environ['PYTHONIOENCODING'] = 'utf-8'

ROOT     = Path(__file__).parent.parent
CFG_FILE = ROOT / "config.json"
SCRIPT   = ROOT / "scripts" / "gee_export.py"


def load_cfg() -> dict:
    if not CFG_FILE.exists():
        print(f"[run_step1] ERROR: config.json not found: {CFG_FILE}")
        sys.exit(1)
    with open(CFG_FILE) as f:
        return json.load(f)


def main():
    print(f"[run_step1] Starting")

    cfg = load_cfg()
    print(f"[run_step1] Config loaded")

    selected = cfg.get("selected_rivers", [])
    if not selected:
        print("[run_step1] ERROR: No rivers selected. Aborting.")
        sys.exit(1)

    if not SCRIPT.exists():
        print(f"[run_step1] ERROR: Script not found: {SCRIPT}")
        print("  → Copy gee_export.py into the scripts/ directory.")
        sys.exit(1)

    try:
        with open(SCRIPT, 'r', encoding='utf-8') as f:
            script_code = f.read()
        print(f"[run_step1] Script loaded: {SCRIPT.name}")
    except Exception as e:
        print(f"[run_step1] ERROR: Failed to read script: {e}")
        sys.exit(1)

    # Build config overrides
    config_vars = {
        'SHAPEFILE_PATH':       cfg.get("shapefile_path", ""),
        'SPECIFIC_RIVERS':      selected,
        'OUTPUT_BASE_FOLDER':   cfg.get("output_base_folder", ""),
        'SENTINEL_SUBFOLDER':   cfg.get("sentinel_subfolder", "Sentinel"),
        'DEM_SUBFOLDER':        cfg.get("dem_subfolder", "DEM"),
        'BUFFER_DISTANCE':      int(cfg.get("buffer_distance", 10000)),
        'RESOLUTION':           int(cfg.get("resolution", 10)),
        'START_DATE':           cfg.get("start_date", "2025-01-01"),
        'END_DATE':             cfg.get("end_date", "2025-12-31"),
        'MAX_CLOUD_COVER':      int(cfg.get("max_cloud_cover", 10)),
        'DRIVE_FOLDER':         cfg.get("drive_folder", "River_Imagery_Batch"),
        'GEE_PROJECT':          cfg.get("gee_project", "plucky-sight-423703-k5"),
        'MAX_CONCURRENT_TASKS': int(cfg.get("max_concurrent_tasks", 100)),
        'SKIP_EXISTING':        bool(cfg.get("skip_existing", True)),
        'SELECTED_BANDS':       cfg.get("selected_bands", ['B2','B3','B4','B5','B6','B7','B8','B8A','B11','B12']),
        'EXPORT_TARGET':        cfg.get("export_target", "drive"),
        'GCS_BUCKET':           'aiq-river-imagery',
        'SAVE_GEOMETRIES_ONLY': False,
        'PROCESS_BATCH_NUMBER': None,
    }

    overrides = "# ── CONFIG INJECTED BY run_step1.py ─────────────────────────────────────\n"
    for var, val in config_vars.items():
        overrides += f"{var} = {repr(val)}\n"
    overrides += "# ─────────────────────────────────────────────────────────────────────────\n"

    # Insert overrides before the first function or class definition
    lines = script_code.split('\n')
    insert_at = 0
    for i, line in enumerate(lines):
        if line.startswith('def ') or line.startswith('class '):
            insert_at = i
            break

    if insert_at == 0:
        print("[run_step1] WARNING: Could not find insertion point — appending overrides at end")
        modified = script_code + "\n" + overrides
    else:
        modified = '\n'.join(lines[:insert_at]) + '\n' + overrides + '\n'.join(lines[insert_at:])

    print(f"[run_step1] Injected config at line {insert_at}")
    print(f"[run_step1] Rivers  : {selected}")
    print(f"[run_step1] Bands   : {config_vars['SELECTED_BANDS']}")
    print(f"[run_step1] Target  : {config_vars['EXPORT_TARGET'].upper()}")
    print(f"[run_step1] Executing gee_export.py ...")
    sys.stdout.flush()

    try:
        exec(compile(modified, str(SCRIPT), "exec"), {
            '__name__':   '__main__',
            '__file__':   str(SCRIPT),
            '__cached__': None,
            '__doc__':    None,
            '__loader__': None,
            '__package__': None,
            '__spec__':   None,
        })
        print("[run_step1] ✓ Script executed successfully")
        sys.exit(0)

    except SystemExit as e:
        raise
    except Exception as e:
        import traceback
        print(f"[run_step1] ERROR: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()