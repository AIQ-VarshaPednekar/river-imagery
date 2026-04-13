"""
dem_clean.py
------------
Open-source replacement for the ArcPy DEM cleaning script.
Works on any machine — no ArcGIS license required.

Usage:
    python dem_clean.py <input_dem> <output_dem>
    python dem_clean.py  (uses hardcoded defaults below)

Requirements:
    pip install rasterio numpy
"""

import sys
import argparse
import rasterio
from rasterio.enums import Resampling
import numpy as np

# ===============================
# ARGUMENT PARSING
# ===============================
parser = argparse.ArgumentParser(description="DEM Cleaner — removes null/black values")
parser.add_argument("input_dem",  nargs="?",
                    default=r"C:\Users\My Pc\Downloads\Kandivali_Sen.tif",
                    help="Path to input DEM GeoTIFF")
parser.add_argument("output_dem", nargs="?",
                    default=r"C:\Users\My Pc\Downloads\Kandivali_Sen_clean.tif",
                    help="Path for cleaned output GeoTIFF")
args = parser.parse_args()

input_dem  = args.input_dem
output_dem = args.output_dem

NODATA_OUT = -9999.0

# ===============================
# READ INPUT
# ===============================
print(f"🔍 Reading raster: {input_dem}")

try:
    with rasterio.open(input_dem) as src:
        dem       = src.read(1).astype("float32")
        profile   = src.profile.copy()
        nodata_in = src.nodata
        crs       = src.crs
        bounds    = src.bounds
except FileNotFoundError:
    print(f"❌ File not found: {input_dem}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error reading file: {e}")
    sys.exit(1)

print(f"   Driver      : {profile.get('driver')}")
print(f"   CRS         : {crs}")
print(f"   Bounds      : {bounds}")
print(f"   Shape       : {dem.shape}")
print(f"   Source NoData: {nodata_in}")
print(f"   Min / Max   : {dem.min():.2f} / {dem.max():.2f}")

if crs is None:
    print("⚠️  WARNING: Input has no CRS (not georeferenced). Output will also have no CRS.")
    print("   If this is a raw nDSM/DSM export, assign the correct CRS in QGIS after.")

# ===============================
# CLEAN — SetNull equivalent
# ===============================
print("⚙️  Removing null / black values...")

mask_nodata = (dem == nodata_in) if nodata_in is not None else np.zeros(dem.shape, dtype=bool)

# ⚠️  Mumbai is coastal — elevation 0 can be real (sea level).
# Disable mask_zero (set all False) if >30% pixels are being removed.
mask_zero = (dem <= 0)

combined_mask = mask_nodata | mask_zero
clean = np.where(combined_mask, NODATA_OUT, dem)

removed = int(combined_mask.sum())
total   = dem.size
print(f"   Pixels removed : {removed:,} / {total:,} ({100*removed/total:.2f}%)")

if removed / total > 0.30:
    print("⚠️  WARNING: >30% of pixels nulled.")
    print("   → In dem_clean.py, change mask_zero to:")
    print("       mask_zero = np.zeros(dem.shape, dtype=bool)   # disabled")

# ===============================
# SAVE OUTPUT
# ===============================
print(f"💾 Saving output: {output_dem}")

# ✅ FIX 1: force GTiff driver regardless of source (JPEG, PNG, etc.)
profile.update(
    driver     = "GTiff",
    dtype      = "float32",
    nodata     = NODATA_OUT,
    count      = 1,
    compress   = "lzw",
    tiled      = True,
    blockxsize = 256,
    blockysize = 256,
)
# Remove JPEG-specific keys invalid for GTiff
for key in ("quality", "jpeg_quality"):
    profile.pop(key, None)

with rasterio.open(output_dem, "w", **profile) as dst:
    dst.write(clean, 1)

# ✅ FIX 2: build overviews AFTER closing the write handle
print("🔺 Building overviews...")
with rasterio.open(output_dem, "r+") as dst:
    dst.build_overviews([2, 4, 8, 16], Resampling.average)
    dst.update_tags(ns="rio_overview", resampling="average")

print(f"✅ SUCCESS: {output_dem}")

# ===============================
# QUICK STATS
# ===============================
valid = clean[clean != NODATA_OUT]
if valid.size > 0:
    print(f"\n📊 Output stats (valid pixels only):")
    print(f"   Min    : {valid.min():.2f} m")
    print(f"   Max    : {valid.max():.2f} m")
    print(f"   Mean   : {valid.mean():.2f} m")
    print(f"   Std Dev: {valid.std():.2f} m")
else:
    print("⚠️  No valid pixels remain — check your masking conditions.")