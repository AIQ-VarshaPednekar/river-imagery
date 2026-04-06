"""
Step 1 Runner - GEE Export
===========================
Reads config.json, then executes gee_export.py script directly,
after setting up configuration and environment.

This runner keeps the original script untouched - no edits needed.

Setup:
    Copy your gee_export.py into the scripts/ directory.
"""

import json
import sys
import os
from pathlib import Path

# Set UTF-8 environment variable to avoid encoding issues
os.environ['PYTHONIOENCODING'] = 'utf-8'

ROOT       = Path(__file__).parent.parent
CFG_FILE   = ROOT / "config.json"
SCRIPT     = ROOT / "scripts" / "gee_export.py"


def load_cfg() -> dict:
    if not CFG_FILE.exists():
        print(f"ERROR: config.json not found: {CFG_FILE}")
        sys.exit(1)
    with open(CFG_FILE) as f:
        return json.load(f)


def main():
    print(f"[run_step1.py] Starting at {Path.cwd()}")
    
    cfg = load_cfg()
    print(f"[run_step1.py] Config loaded")

    if not SCRIPT.exists():
        print(f"ERROR: Script not found: {SCRIPT}")
        print("  -> Copy gee_export.py into the scripts/ directory.")
        sys.exit(1)

    # Read the gee_export.py script
    try:
        with open(SCRIPT, 'r', encoding='utf-8') as f:
            script_code = f.read()
        print(f"[run_step1.py] Script loaded: {SCRIPT.name}")
    except Exception as e:
        print(f"ERROR: Failed to read {SCRIPT.name}: {e}")
        sys.exit(1)

    # Parse selected rivers from config
    selected = cfg.get("selected_rivers", [])
    if not selected:
        print("ERROR: No rivers selected. Aborting.")
        sys.exit(1)

    # Build config override statements
    config_vars = {
        'SHAPEFILE_PATH': cfg.get("shapefile_path", ""),
        'SPECIFIC_RIVERS': selected,
        'OUTPUT_BASE_FOLDER': cfg.get("output_base_folder", ""),
        'SENTINEL_SUBFOLDER': cfg.get("sentinel_subfolder", "Sentinel"),
        'DEM_SUBFOLDER': cfg.get("dem_subfolder", "DEM"),
        'BUFFER_DISTANCE': int(cfg.get("buffer_distance", 10000)),
        'RESOLUTION': int(cfg.get("resolution", 10)),
        'START_DATE': cfg.get("start_date", "2025-01-01"),
        'END_DATE': cfg.get("end_date", "2025-12-31"),
        'MAX_CLOUD_COVER': int(cfg.get("max_cloud_cover", 10)),
        'DRIVE_FOLDER': cfg.get("drive_folder", "River_Imagery_Batch"),
        'GEE_PROJECT': cfg.get("gee_project", "plucky-sight-423703-k5"),
        'MAX_CONCURRENT_TASKS': int(cfg.get("max_concurrent_tasks", 100)),
        'SKIP_EXISTING': bool(cfg.get("skip_existing", True)),
        'SAVE_GEOMETRIES_ONLY': False,
        'PROCESS_BATCH_NUMBER': None,
    }
    
    config_overrides = "# CONFIG OVERRIDES FROM run_step1.py\n"
    for var_name, var_value in config_vars.items():
        config_overrides += f"{var_name} = {repr(var_value)}\n"

    # Find the line after the configuration section (before function definitions)
    # Look for the "FUNCTIONS" comment
    lines = script_code.split('\n')
    insert_index = 0
    for i, line in enumerate(lines):
        if '# =============================================================================\n' in script_code[sum(len(l)+1 for l in lines[:i]):]:
            if 'FUNCTIONS' in script_code[sum(len(l)+1 for l in lines[:i]):sum(len(l)+1 for l in lines[:i+20])]:
                insert_index = i
                break
    
    if insert_index == 0:
        # Fallback: insert before the first function definition
        for i, line in enumerate(lines):
            if line.startswith('def '):
                insert_index = i
                break
    
    print(f"[run_step1.py] Inserting config overrides at line {insert_index}...")
    modified_script = '\n'.join(lines[:insert_index]) + '\n' + config_overrides + '\n'.join(lines[insert_index:])
    
    print(f"[run_step1.py] Executing modified gee_export script...")
    sys.stdout.flush()
    
    try:
        # Create a namespace with necessary globals
        script_globals = {
            '__name__': '__main__',
            '__file__': str(SCRIPT),
            '__cached__': None,
            '__doc__': None,
            '__loader__': None,
            '__package__': None,
            '__spec__': None,
        }
        
        # Execute the script
        exec(modified_script, script_globals)
        
        print(f"[run_step1.py] Script executed successfully")
        sys.exit(0)
        
    except SystemExit as e:
        # Re-raise system exit as-is
        raise
    except Exception as e:
        print(f"ERROR: Error during execution: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()