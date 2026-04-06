#!/usr/bin/env python
"""Test if venv Python works and can import required modules."""

import sys
import json
from pathlib import Path

print("=" * 60)
print("VENV TEST RUNNER")
print("=" * 60)
print(f"Python: {sys.executable}")
print(f"Version: {sys.version}")
print()

# Test 1: Import config
ROOT = Path(__file__).parent  # This script is in the root project dir
CFG_FILE = ROOT / "config.json"

print("TEST 1: Load config")
print(f"  Script location: {__file__}")
print(f"  ROOT: {ROOT}")
print(f"  CFG_FILE: {CFG_FILE}")
try:
    with open(CFG_FILE) as f:
        cfg = json.load(f)
    print(f"  OK: Loaded {CFG_FILE}")
    print(f"      Selected rivers: {cfg.get('selected_rivers', [])}")
except Exception as e:
    print(f"  ERROR: {e}")
    sys.exit(1)

# Test 2: Import gee_export module
print("\nTEST 2: Import gee_export module")
try:
    import importlib.util
    SCRIPT = ROOT / "scripts" / "gee_export.py"
    spec = importlib.util.spec_from_file_location("gee_export", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    print(f"  OK: Loaded {SCRIPT.name}")
    print(f"      SHAPEFILE_PATH: {mod.SHAPEFILE_PATH}")
    print(f"      SPECIFIC_RIVERS: {mod.SPECIFIC_RIVERS}")
except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 3: Check shapefile exists
print("\nTEST 3: Check shapefile exists")
try:
    shapefile = Path(mod.SHAPEFILE_PATH)
    if shapefile.exists():
        print(f"  OK: Shapefile exists at {shapefile}")
    else:
        print(f"  ERROR: Shapefile not found at {shapefile}")
        sys.exit(1)
except Exception as e:
    print(f"  ERROR: {e}")
    sys.exit(1)

# Test 4: Try loading shapefile
print("\nTEST 4: Load shapefile with geopandas")
try:
    import geopandas as gpd
    gdf = gpd.read_file(mod.SHAPEFILE_PATH)
    print(f"  OK: Loaded {len(gdf)} rivers from shapefile")
    print(f"      Columns: {list(gdf.columns)}")
except Exception as e:
    print(f"  ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 60)
print("ALL TESTS PASSED")
print("=" * 60)
