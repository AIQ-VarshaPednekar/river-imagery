"""
Step 2 Runner — Google Drive Download
=======================================
Reads config.json, loads scripts/drive_download.py, overrides its configuration
globals with values from config.json, then calls drive_download.main().

Setup:
    Copy your drive_download.py into the scripts/ directory.
"""

import json
import sys
import importlib.util
from pathlib import Path

ROOT       = Path(__file__).parent.parent
CFG_FILE   = ROOT / "config.json"
SCRIPT     = ROOT / "scripts" / "drive_download.py"


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
        print("  → Copy drive_download.py into the scripts/ directory.")
        sys.exit(1)

    print(f"Loading {SCRIPT.name} …")
    spec = importlib.util.spec_from_file_location("drive_download", SCRIPT)
    mod  = importlib.util.module_from_spec(spec)  # type: ignore
    spec.loader.exec_module(mod)                   # type: ignore

    # Build local output paths from config
    output_base  = cfg.get("output_base_folder", getattr(mod, "SENTINEL_LOCAL", ""))
    sentinel_sub = cfg.get("sentinel_subfolder", "Sentinel")
    dem_sub      = cfg.get("dem_subfolder",      "DEM")

    import os
    sentinel_local = os.path.join(output_base, sentinel_sub) if output_base else getattr(mod, "SENTINEL_LOCAL", "")
    dem_local      = os.path.join(output_base, dem_sub)      if output_base else getattr(mod, "DEM_LOCAL",      "")

    # ── Override config ────────────────────────────────────────────────────
    mod.TOKEN_FILE             = cfg.get("token_file",             getattr(mod, "TOKEN_FILE",             ""))
    mod.CREDENTIALS_FILE       = cfg.get("credentials_file",       getattr(mod, "CREDENTIALS_FILE",       ""))
    mod.SENTINEL_LOCAL         = sentinel_local
    mod.DEM_LOCAL              = dem_local
    mod.DRIVE_SENTINEL_FOLDER  = cfg.get("drive_sentinel_folder",  getattr(mod, "DRIVE_SENTINEL_FOLDER",  "River_Imagery_Batch/Sentinel"))
    mod.DRIVE_DEM_FOLDER       = cfg.get("drive_dem_folder",       getattr(mod, "DRIVE_DEM_FOLDER",       "River_Imagery_Batch/DEM"))

    print(f"✓ Config applied")
    print(f"  Credentials : {mod.CREDENTIALS_FILE}")
    print(f"  Token file  : {mod.TOKEN_FILE}")
    print(f"  Sentinel →  {mod.SENTINEL_LOCAL}")
    print(f"  DEM      →  {mod.DEM_LOCAL}")
    print(f"  Drive sentinel folder: {mod.DRIVE_SENTINEL_FOLDER}")
    print(f"  Drive DEM folder     : {mod.DRIVE_DEM_FOLDER}")

    mod.main()


if __name__ == "__main__":
    main()