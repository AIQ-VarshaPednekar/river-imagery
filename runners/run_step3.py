"""
run_step3.py — Step 3 Runner: Merge GeoTIFF Tiles
==================================================
PURPOSE:
  Merge the individual GeoTIFF tile files downloaded in Step 2 into a single
  merged GeoTIFF per river. GEE splits large exports into multiple tiles named
  like "Ganga_sentinel-000.tif", "Ganga_sentinel-001.tif", etc.
  This step combines them into one "Ganga_sentinel_merged.tif".

WHY a separate runner (instead of running merge_tiles.py directly)?
  merge_tiles.py has HARDCODED path variables (input_folder, output_folder) and
  runs its merge loop at MODULE LEVEL (not inside a function). This means:
    - We can't import it and call a main() like run_step2.py does.
    - We can't pass paths as arguments because it uses variables, not argparse.

  Strategy:
    1. Read merge_tiles.py source as text
    2. Use regex to REPLACE the hardcoded input_folder / output_folder assignments
       with the correct paths from config.json
    3. exec() the patched source in a clean namespace

DATA FLOW:
  config.json
      ↓ (output_base_folder + sentinel_subfolder)
  run_step3.py
      ↓ (patches merge_tiles.py source with correct paths)
  merge_tiles.py (exec'd)
      ↓ (reads tiles from Sentinel/ folder)
      ↓ (merges per-river into Sentinel_Merged/)
  local disk: output_base_folder/Sentinel_Merged/*_merged.tif

CALLED BY:
  main.py → _run_runner("run_step3.py") → subprocess.Popen([python, "run_step3.py"])

MEMORY EFFICIENCY:
  merge_tiles.py uses chunked windowed I/O — it processes 2048 rows at a time.
  This keeps RAM usage bounded even for very large rivers (multi-GB output files).

PROJ / GDAL PATH FIX:
  On Windows, if PostgreSQL is installed, its proj.db can conflict with rasterio's proj.db.
  We pre-set PROJ_DATA and PROJ_LIB to pyproj's data directory BEFORE importing rasterio,
  ensuring rasterio uses the correct PROJ database.
"""

import os              # os.makedirs to create output folder, os.path.join for paths

# ── PROJ/GDAL path fix (MUST come before any rasterio / pyproj import) ────────
# pyproj ships with its own proj.db. We tell the PROJ library to use pyproj's
# copy by setting PROJ_DATA and PROJ_LIB environment variables.
# This prevents the "proj: pj_obj_create: cannot find proj.db" error that occurs
# when PostgreSQL's PROJ installation takes precedence.
import pyproj
os.environ["PROJ_DATA"] = pyproj.datadir.get_data_dir()
os.environ["PROJ_LIB"]  = pyproj.datadir.get_data_dir()

import json            # Parse config.json
import sys             # sys.exit() to signal success/failure
import os              # (re-imported after pyproj, but that's fine — same module)
import importlib.util  # Not actually used (leftover import) — kept for consistency
from pathlib import Path   # Platform-safe path handling

# ── Path resolution ───────────────────────────────────────────────────────────
ROOT       = Path(__file__).parent.parent           # Project root
CFG_FILE   = ROOT / "config.json"                   # Config file
SCRIPT     = ROOT / "scripts" / "merge_tiles.py"    # The merge script


def load_cfg() -> dict:
    """Read config.json and return as dict. Exit 1 if missing."""
    if not CFG_FILE.exists():
        print(f"✗ config.json not found: {CFG_FILE}")
        sys.exit(1)
    with open(CFG_FILE) as f:
        return json.load(f)


def main():
    """Patch merge_tiles.py with correct paths from config, then exec it."""
    cfg = load_cfg()

    if not SCRIPT.exists():
        print(f"✗ Script not found: {SCRIPT}")
        print("  → Copy merge_tiles.py into the scripts/ directory.")
        sys.exit(1)

    # ── Build input/output folder paths ───────────────────────────────────────
    # Where tiles were downloaded to (by Step 2):
    #   input_folder = output_base_folder/Sentinel/
    # Where merged files will be written:
    #   output_folder = output_base_folder/Sentinel_Merged/
    # Note: output_folder is NOT a config key — always "Sentinel_Merged" (hardcoded convention).
    output_base  = cfg.get("output_base_folder", "")
    sentinel_sub = cfg.get("sentinel_subfolder", "Sentinel")

    input_folder  = os.path.join(output_base, sentinel_sub)       if output_base else ""
    output_folder = os.path.join(output_base, "Sentinel_Merged")  if output_base else ""

    # Validate that the input folder actually exists (Step 2 must have run first)
    if not input_folder or not os.path.exists(input_folder):
        print(f"✗ Input folder not found: {input_folder}")
        print("  → Make sure Step 2 completed successfully and output_base_folder is correct.")
        sys.exit(1)

    # Create the output folder if it doesn't exist yet
    os.makedirs(output_folder, exist_ok=True)

    print(f"Loading {SCRIPT.name} …")

    # ── Read merge_tiles.py source as a string ────────────────────────────────
    with open(SCRIPT, encoding='utf-8') as f:
        source = f.read()

    # ── Patch the source: replace hardcoded folder assignments ────────────────
    # merge_tiles.py contains lines like:
    #   input_folder  = ""
    #   output_folder = ""
    # We replace these assignments with the real paths using regex substitution.
    # The lambda is used because re.sub with a replacement string would interpret
    # backslashes in Windows paths as escape sequences — the lambda avoids that.

    overrides = (
        f'\ninput_folder  = r"{input_folder}"\n'    # r"..." prefix handles Windows backslashes
        f'output_folder = r"{output_folder}"\n'
    )

    import re

    # Replace the existing assignments if they exist in the source
    patched = re.sub(
        r'input_folder\s*=\s*r?"[^"]*"',            # Matches: input_folder = "" or input_folder = r"..."
        lambda _: f'input_folder  = r"{input_folder}"',
        source
    )
    patched = re.sub(
        r'output_folder\s*=\s*r?"[^"]*"',            # Matches: output_folder = "" or output_folder = r"..."
        lambda _: f'output_folder = r"{output_folder}"',
        patched
    )

    if patched == source:
        # The regex didn't find any existing assignments to replace.
        # This can happen if someone changed the variable names — fall back to appending.
        patched = source + overrides

    print(f"✓ Config applied")
    print(f"  Input  : {input_folder}")
    print(f"  Output : {output_folder}")

    # ── Execute the patched source ─────────────────────────────────────────────
    # We exec() in a fresh namespace dict.
    # __name__ = '__main__' triggers merge_tiles.py's module-level merge loop.
    # merge_tiles.py doesn't have `if __name__ == "__main__"` — it runs at module level.
    # After exec() finishes, all merges have been written.
    ns = {"__name__": "__main__", "__file__": str(SCRIPT)}
    exec(compile(patched, str(SCRIPT), "exec"), ns)

    # ── Verify output ──────────────────────────────────────────────────────────
    # After exec finishes, check that at least one merged file was created.
    # If no *_merged.tif files exist, the merge loop likely failed silently.
    merged_files = list(Path(output_folder).glob("*_merged.tif"))
    if not merged_files:
        print("✗ No merged files were produced — merge likely failed.")
        sys.exit(1)
    # Success: at least one merged file exists → exit 0 tells main.py step 3 succeeded


if __name__ == "__main__":
    main()