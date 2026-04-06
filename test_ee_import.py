#!/usr/bin/env python
"""Test if ee import hangs."""

import sys
print(f"Start: Attempting to import ee...")
sys.stdout.flush()

try:
    import ee
    print("OK: ee imported successfully")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
