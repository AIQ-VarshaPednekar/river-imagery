"""
run_step1.py — Step 1 Runner: GEE Export
=========================================
PURPOSE:
  This is the bridge between the FastAPI backend (main.py) and the actual
  Google Earth Engine export script (scripts/gee_export.py).

WHY a separate runner instead of importing gee_export.py directly?
  gee_export.py uses MODULE-LEVEL configuration variables (e.g. SHAPEFILE_PATH,
  SPECIFIC_RIVERS). These are plain Python variables at the top of the file.
  We need to OVERRIDE them at runtime with values from config.json before
  the script's main() logic runs.

  The cleanest way is to:
    1. Read the script source code as text (open().read())
    2. Inject override assignments into the source
    3. compile() + exec() the modified source in a fresh namespace

  This avoids having to refactor gee_export.py to use function arguments
  or a config dict — keeping it standalone (runnable by itself from the command line).

DATA FLOW:
  config.json
      ↓  (load_cfg reads all keys)
  run_step1.py
      ↓  (builds config_vars dict, injects into gee_export.py source)
  gee_export.py (compiled + exec'd)
      ↓  (calls GEE API)
  Google Earth Engine → exports tasks started

CALLED BY:
  main.py → _run_runner("run_step1.py") → subprocess.Popen([python, "run_step1.py"])
"""

import json           # To parse config.json
import sys            # sys.exit() to report failure codes to the parent process
import os             # os.environ to set encoding
import re             # (imported but not used here — kept for consistency with other runners)
from pathlib import Path   # Platform-safe path construction

# Force UTF-8 output encoding so print() works correctly on Windows.
# Without this, Windows uses cp1252 by default, which can't encode many Unicode chars.
os.environ['PYTHONIOENCODING'] = 'utf-8'

# ── Path resolution ────────────────────────────────────────────────────────────
# __file__ is the path to this runner file (e.g. .../runners/run_step1.py)
# .parent.parent navigates up two levels to the project root
ROOT     = Path(__file__).parent.parent          # → .../river project aiq/
CFG_FILE = ROOT / "config.json"                  # Shared config file
SCRIPT   = ROOT / "scripts" / "gee_export.py"   # The actual export script


def load_cfg() -> dict:
    """Read and return config.json as a Python dict.

    Why exit(1) instead of raising an exception?
      This script runs as a subprocess. The parent process (main.py) inspects
      the exit code. exit(1) tells it the step failed; exit(0) = success.
      Exceptions that reach the top level would also exit(1) but with a messy traceback.
    """
    if not CFG_FILE.exists():
        print(f"[run_step1] ERROR: config.json not found: {CFG_FILE}")
        sys.exit(1)
    with open(CFG_FILE) as f:
        return json.load(f)


def main():
    """Main entry point: load config, inject overrides, execute gee_export.py."""
    print(f"[run_step1] Starting")

    cfg = load_cfg()
    print(f"[run_step1] Config loaded")

    # Validate that rivers were selected before launching
    # (main.py's /api/pipeline/step1 writes "selected_rivers" to config.json
    #  before spawning this runner, so it should always be set)
    selected = cfg.get("selected_rivers", [])
    if not selected:
        print("[run_step1] ERROR: No rivers selected. Aborting.")
        sys.exit(1)

    # Validate the export script exists on disk
    if not SCRIPT.exists():
        print(f"[run_step1] ERROR: Script not found: {SCRIPT}")
        print("  → Copy gee_export.py into the scripts/ directory.")
        sys.exit(1)

    # ── Read gee_export.py source as a string ─────────────────────────────────
    try:
        with open(SCRIPT, 'r', encoding='utf-8') as f:
            script_code = f.read()
        print(f"[run_step1] Script loaded: {SCRIPT.name}")
    except Exception as e:
        print(f"[run_step1] ERROR: Failed to read script: {e}")
        sys.exit(1)

    # ── Build the config override block ───────────────────────────────────────
    # config_vars maps each module-level variable in gee_export.py to the
    # value it should have from config.json.
    # We use repr(val) to convert Python values to their literal representation:
    #   e.g. repr("my_path") → '"my_path"'
    #        repr(10000)      → '10000'
    #        repr(["B2"])     → '["B2"]'
    config_vars = {
        'SHAPEFILE_PATH':       cfg.get("shapefile_path", ""),
        'SPECIFIC_RIVERS':      selected,                              # List of selected river names
        'OUTPUT_BASE_FOLDER':   cfg.get("output_base_folder", ""),
        'SENTINEL_SUBFOLDER':   cfg.get("sentinel_subfolder", "Sentinel"),
        'DEM_SUBFOLDER':        cfg.get("dem_subfolder", "DEM"),
        'BUFFER_DISTANCE':      int(cfg.get("buffer_distance", 10000)),   # int() prevents float "10000.0"
        'RESOLUTION':           int(cfg.get("resolution", 10)),
        'START_DATE':           cfg.get("start_date", "2025-01-01"),
        'END_DATE':             cfg.get("end_date", "2025-12-31"),
        'MAX_CLOUD_COVER':      int(cfg.get("max_cloud_cover", 10)),
        'DRIVE_FOLDER':         cfg.get("drive_folder", "River_Imagery_Batch"),
        'GEE_PROJECT':          cfg.get("gee_project", "plucky-sight-423703-k5"),
        'MAX_CONCURRENT_TASKS': int(cfg.get("max_concurrent_tasks", 100)),
        'SKIP_EXISTING':        bool(cfg.get("skip_existing", True)),
        'SELECTED_BANDS':       cfg.get("selected_bands", ['B2','B3','B4','B5','B6','B7','B8','B8A','B11','B12']),
        'EXPORT_TARGET':        cfg.get("export_target", "drive"),    # "drive" | "gcs" | "both"
        'GCS_BUCKET':           'aiq-river-imagery',                  # Hardcoded (not in config UI)
        'SAVE_GEOMETRIES_ONLY': False,                                # Not exposed in UI
        'PROCESS_BATCH_NUMBER': None,                                 # Not exposed in UI
    }

    # Build the override block as a string of Python assignment statements.
    # This block will be inserted into the source code of gee_export.py BEFORE
    # the first function or class definition, so it overrides the module-level defaults.
    overrides = "# ── CONFIG INJECTED BY run_step1.py ─────────────────────────────────────\n"
    for var, val in config_vars.items():
        overrides += f"{var} = {repr(val)}\n"   # e.g. 'RESOLUTION = 10'
    overrides += "# ─────────────────────────────────────────────────────────────────────────\n"

    # ── Find insertion point in the script ────────────────────────────────────
    # We insert the overrides BEFORE the first 'def' or 'class' line.
    # Why? Module-level code runs before functions are called. If we insert
    # AFTER the function definitions but BEFORE main(), it wouldn't override
    # the variables used inside the functions. Inserting before the first def
    # ensures the overrides are in scope when main() reads them.
    lines = script_code.split('\n')
    insert_at = 0
    for i, line in enumerate(lines):
        if line.startswith('def ') or line.startswith('class '):
            insert_at = i   # Found the first function/class — insert just before here
            break

    if insert_at == 0:
        # Fallback: no function found (unusual) — append overrides at the end
        print("[run_step1] WARNING: Could not find insertion point — appending overrides at end")
        modified = script_code + "\n" + overrides
    else:
        # Join the lines before the first def, prepend our overrides, then the rest
        modified = '\n'.join(lines[:insert_at]) + '\n' + overrides + '\n'.join(lines[insert_at:])

    print(f"[run_step1] Injected config at line {insert_at}")
    print(f"[run_step1] Rivers  : {selected}")
    print(f"[run_step1] Bands   : {config_vars['SELECTED_BANDS']}")
    print(f"[run_step1] Target  : {config_vars['EXPORT_TARGET'].upper()}")
    print(f"[run_step1] Executing gee_export.py ...")
    sys.stdout.flush()   # Flush before exec() so logs appear before any script output

    # ── Execute the modified script ────────────────────────────────────────────
    # compile() converts the string source code to a code object (faster than exec(string) directly).
    # exec() runs the code object in the provided globals dict.
    # We set __name__ = '__main__' so gee_export.py's `if __name__ == "__main__": main()` block fires.
    # We set __file__ = str(SCRIPT) so relative imports and path logic inside gee_export.py work.
    try:
        exec(compile(modified, str(SCRIPT), "exec"), {
            '__name__':    '__main__',
            '__file__':    str(SCRIPT),
            '__cached__':  None,
            '__doc__':     None,
            '__loader__':  None,
            '__package__': None,
            '__spec__':    None,
        })
        print("[run_step1] ✓ Script executed successfully")
        sys.exit(0)   # Signal success to the parent process (main.py)

    except SystemExit as e:
        raise   # Let SystemExit propagate — gee_export.py may call sys.exit() intentionally

    except Exception as e:
        import traceback
        print(f"[run_step1] ERROR: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)   # Signal failure to main.py


if __name__ == "__main__":
    main()