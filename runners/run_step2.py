"""
run_step2.py — Step 2 Runner: Google Drive Download
=====================================================
PURPOSE:
  Download satellite imagery tiles that Google Earth Engine exported to Google Drive.

  After Step 1 (gee_export.py) submits export tasks to GEE, GEE runs them
  asynchronously and saves the results as GeoTIFF files in Google Drive.
  This script:
    1. Authenticates with Google Drive (using OAuth2 credentials)
    2. Finds the Sentinel and DEM folders in Drive
    3. Downloads all .tif files to local disk
    4. Supports RESUME: if a file was partially downloaded, it continues from where it stopped.

WHY a separate runner file (instead of just running drive_download.py directly)?
  drive_download.py has hardcoded path variables at the top (TOKEN_FILE, SENTINEL_LOCAL, etc.).
  We need to override them at runtime using values from config.json.
  Unlike run_step1.py (which uses exec()), this runner uses importlib to load the script
  as a Python MODULE, then directly monkey-patches the module's global variables.
  This works because drive_download.py wraps all logic in a main() function, so
  importing it doesn't immediately run any I/O.

DATA FLOW:
  config.json
      ↓ (load_cfg reads paths and credentials)
  run_step2.py
      ↓ (imports drive_download.py as module, overrides its globals)
  drive_download.py → Google Drive API
      ↓ (downloads .tif files)
  local disk: output_base_folder/Sentinel/*.tif
              output_base_folder/DEM/*.tif

CALLED BY:
  main.py → _run_runner("run_step2.py") → subprocess.Popen([python, "run_step2.py"])

RESUME LOGIC:
  If a file already exists locally and matches Drive's file size → skip it.
  If a file exists but is smaller than Drive size → resume using HTTP Range header.
  (Implemented in drive_download.py's download_file() function)
"""

import json              # Parse config.json
import sys               # sys.exit() to signal success/failure to parent process
import importlib.util    # Load drive_download.py as a module without adding it to sys.path
from pathlib import Path  # Platform-safe path handling

# ── Path resolution ───────────────────────────────────────────────────────────
# __file__ = .../runners/run_step2.py
# .parent.parent = project root
ROOT       = Path(__file__).parent.parent           # → .../river project aiq/
CFG_FILE   = ROOT / "config.json"                   # Shared configuration file
SCRIPT     = ROOT / "scripts" / "drive_download.py" # The actual download script


def load_cfg() -> dict:
    """Read config.json and return it as a Python dict.

    Exits with code 1 if the file doesn't exist, signalling failure to the
    parent process (main.py) which reads the subprocess exit code.
    """
    if not CFG_FILE.exists():
        print(f"✗ config.json not found: {CFG_FILE}")
        sys.exit(1)
    with open(CFG_FILE) as f:
        return json.load(f)


def main():
    """Load config, import drive_download.py, override its globals, run its main()."""
    cfg = load_cfg()

    # Validate that the drive_download.py script exists
    if not SCRIPT.exists():
        print(f"✗ Script not found: {SCRIPT}")
        print("  → Copy drive_download.py into the scripts/ directory.")
        sys.exit(1)

    print(f"Loading {SCRIPT.name} …")

    # ── Import drive_download.py as a module ──────────────────────────────────
    # importlib.util.spec_from_file_location creates a ModuleSpec for a file at a specific path.
    # This is equivalent to `import drive_download` but without adding the scripts/ dir to sys.path.
    # "drive_download" is the module name we assign to it in Python's module registry.
    spec = importlib.util.spec_from_file_location("drive_download", SCRIPT)

    # module_from_spec creates the empty module object from the spec
    mod  = importlib.util.module_from_spec(spec)  # type: ignore

    # exec_module actually executes the module's source code.
    # After this call, all module-level code in drive_download.py has run:
    #   - socket.setdefaulttimeout(300) is set
    #   - SCOPES, TOKEN_FILE, etc. are defined as empty strings
    #   - All functions (get_creds, download_file, main, ...) are defined
    # BUT no actual download has started yet, because those are inside main().
    spec.loader.exec_module(mod)  # type: ignore

    # ── Build local output paths from config ───────────────────────────────────
    # The local paths where tiles will be saved are combinations of:
    #   output_base_folder (e.g. "C:/Users/.../Imagery_Output")
    #   sentinel_subfolder (e.g. "Sentinel")
    #   dem_subfolder      (e.g. "DEM")
    # → tiles go to: Imagery_Output/Sentinel/*.tif and Imagery_Output/DEM/*.tif
    output_base  = cfg.get("output_base_folder", getattr(mod, "SENTINEL_LOCAL", ""))
    sentinel_sub = cfg.get("sentinel_subfolder", "Sentinel")
    dem_sub      = cfg.get("dem_subfolder",      "DEM")

    import os
    # os.path.join builds the full local path; fall back to drive_download.py's default if not set
    sentinel_local = os.path.join(output_base, sentinel_sub) if output_base else getattr(mod, "SENTINEL_LOCAL", "")
    dem_local      = os.path.join(output_base, dem_sub)      if output_base else getattr(mod, "DEM_LOCAL",      "")

    # ── Override drive_download.py's module globals ────────────────────────────
    # By assigning to mod.VARIABLE, we override the module's global variable
    # in-place. When drive_download.main() later reads these globals, it sees
    # the config.json values rather than the empty string defaults.
    mod.TOKEN_FILE             = cfg.get("token_file",            getattr(mod, "TOKEN_FILE",             ""))
    mod.CREDENTIALS_FILE       = cfg.get("credentials_file",      getattr(mod, "CREDENTIALS_FILE",       ""))
    mod.SENTINEL_LOCAL         = sentinel_local                    # Local folder for Sentinel tiles
    mod.DEM_LOCAL              = dem_local                         # Local folder for DEM tiles
    mod.DRIVE_SENTINEL_FOLDER  = cfg.get("drive_sentinel_folder", getattr(mod, "DRIVE_SENTINEL_FOLDER",  "River_Imagery_Batch/Sentinel"))
    mod.DRIVE_DEM_FOLDER       = cfg.get("drive_dem_folder",      getattr(mod, "DRIVE_DEM_FOLDER",       "River_Imagery_Batch/DEM"))

    # Log the final resolved config so user can verify paths in the live console
    print(f"✓ Config applied")
    print(f"  Credentials : {mod.CREDENTIALS_FILE}")
    print(f"  Token file  : {mod.TOKEN_FILE}")
    print(f"  Sentinel →  {mod.SENTINEL_LOCAL}")
    print(f"  DEM      →  {mod.DEM_LOCAL}")
    print(f"  Drive sentinel folder: {mod.DRIVE_SENTINEL_FOLDER}")
    print(f"  Drive DEM folder     : {mod.DRIVE_DEM_FOLDER}")

    # ── Run the download ───────────────────────────────────────────────────────
    # Call drive_download.py's main() with the overridden globals.
    # This authenticates to Google Drive, lists files, and downloads them.
    # If mod.main() raises an exception or calls sys.exit(1), this runner
    # will also exit non-zero, signalling failure to main.py.
    mod.main()


if __name__ == "__main__":
    main()