#!/usr/bin/env python
"""Test loading gee_export module."""

import sys
import importlib.util
from pathlib import Path

ROOT = Path(__file__).parent
SCRIPT_PATH = ROOT / "scripts" / "gee_export.py"

print(f"Start: Loading {SCRIPT_PATH}...")
sys.stdout.flush()

try:
    spec = importlib.util.spec_from_file_location("gee_export", SCRIPT_PATH)
    if spec is None:
        print("ERROR: Could not create module spec")
        sys.exit(1)
    
    mod = importlib.util.module_from_spec(spec)
    sys.modules["gee_export"] = mod
    
    print("Spec created, now executing module...")
    sys.stdout.flush()
    
    spec.loader.exec_module(mod)
    
    print("OK: Module loaded successfully")
    print(f"    SHAPEFILE_PATH: {mod.SHAPEFILE_PATH}")
    print(f"    SPECIFIC_RIVERS: {mod.SPECIFIC_RIVERS}")
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
