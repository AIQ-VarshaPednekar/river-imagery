#!/usr/bin/env python
"""Test if all required modules can be imported."""

import sys

modules_to_test = [
    "ee",
    "geopandas",
    "pandas",
    "shapely",
    "rasterio",
    "numpy",
    "sqlalchemy",
    "psycopg2",
    "google.auth",
    "google_auth_oauthlib",
    "google.auth.transport.requests",
    "googleapiclient",
    "requests",
    "fastapi",
    "uvicorn",
]

print("Testing module imports...")
print("-" * 50)

all_ok = True
for module in modules_to_test:
    try:
        __import__(module)
        print(f"OK: {module}")
    except ImportError as e:
        print(f"FAIL: {module} - {e}")
        all_ok = False

print("-" * 50)
if all_ok:
    print("SUCCESS: All modules imported successfully!")
    sys.exit(0)
else:
    print("FAILED: Some modules are missing. Please install requirements.txt")
    sys.exit(1)
