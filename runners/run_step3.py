"""
Step 3 Runner — Merge GeoTIFF Tiles
=====================================
Reads config.json, loads scripts/merge_tiles.py, overrides its input/output
folder paths with values from config.json, then re-runs its merge loop.

Setup:
    Copy your merge_tiles.py into the scripts/ directory.

Note:
    merge_tiles.py does not have a main() function — it runs its logic at
    module level. This runner therefore patches the module globals and then
    re-executes the merge loop using the patched values.
"""

import os
import pyproj
os.environ["PROJ_DATA"] = pyproj.datadir.get_data_dir()
os.environ["PROJ_LIB"]  = pyproj.datadir.get_data_dir()
import json
import sys
import os
import importlib.util
from pathlib import Path

ROOT       = Path(__file__).parent.parent
CFG_FILE   = ROOT / "config.json"
SCRIPT     = ROOT / "scripts" / "merge_tiles.py"


def load_cfg() -> dict:
    if not CFG_FILE.exists():
        print(f"✗ config.json not found: {CFG_FILE}")
        sys.exit(1)
    with open(CFG_FILE) as f:
        return json.load(f)


def main():
    cfg = load_cfg()

    if not SCRIPT.exists():
        print(f"✗ Script not found: {SCRIPT}")
        print("  → Copy merge_tiles.py into the scripts/ directory.")
        sys.exit(1)

    output_base  = cfg.get("output_base_folder", "")
    sentinel_sub = cfg.get("sentinel_subfolder", "Sentinel")

    input_folder  = os.path.join(output_base, sentinel_sub)  if output_base else ""
    output_folder = os.path.join(output_base, "Sentinel_Merged") if output_base else ""

    if not input_folder or not os.path.exists(input_folder):
        print(f"✗ Input folder not found: {input_folder}")
        print("  → Make sure Step 2 completed successfully and output_base_folder is correct.")
        sys.exit(1)

    os.makedirs(output_folder, exist_ok=True)

    print(f"Loading {SCRIPT.name} …")

    # ── Load module (this runs module-level code including the merge loop,
    #    but we need to patch paths BEFORE that happens).
    #    Strategy: read the source, inject overrides at the top, then exec.
    # ─────────────────────────────────────────────────────────────────────────
    with open(SCRIPT, encoding='utf-8') as f:
        source = f.read()

    # Inject path overrides right before the main logic runs.
    # We replace the hardcoded assignment lines if present, otherwise prepend.
    overrides = (
        f'\ninput_folder  = r"{input_folder}"\n'
        f'output_folder = r"{output_folder}"\n'
    )

    # Try to replace existing assignments
    import re
    patched = re.sub(r'input_folder\s*=\s*r?"[^"]*"',  lambda _: f'input_folder  = r"{input_folder}"',  source)
    patched = re.sub(r'output_folder\s*=\s*r?"[^"]*"', lambda _: f'output_folder = r"{output_folder}"', patched)

    if patched == source:
        # Assignments not found — prepend overrides after imports block
        patched = source + overrides

    print(f"✓ Config applied")
    print(f"  Input  : {input_folder}")
    print(f"  Output : {output_folder}")

    # Execute patched source in a clean namespace
    ns = {"__name__": "__main__", "__file__": str(SCRIPT)}
    exec(compile(patched, str(SCRIPT), "exec"), ns)
    # After exec(...):
    merged_files = list(Path(output_folder).glob("*_merged.tif"))
    if not merged_files:
        print("✗ No merged files were produced — merge likely failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()