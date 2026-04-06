import os

# ── Fix PROJ conflict with PostgreSQL's proj.db ───────────────────────────────
# Must be set BEFORE any rasterio / pyproj import
# import pyproj
# os.environ["PROJ_DATA"] = pyproj.datadir.get_data_dir()
# os.environ["PROJ_LIB"]  = pyproj.datadir.get_data_dir()
# ─────────────────────────────────────────────────────────────────────────────
for _var in ("PROJ_DATA", "PROJ_LIB", "PROJ_DIR"):
    os.environ.pop(_var, None)

import glob
import numpy as np
import rasterio
from rasterio.merge import merge
from rasterio.transform import from_bounds
from rasterio.warp import calculate_default_transform, reproject, Resampling as WarpResampling
# from osgeo import gdal

# gdal.UseExceptions()

TARGET_CRS = "EPSG:4326"  # ← change to your river's UTM zone (check your tiles)
NODATA_VAL = -9999         # ← safer than 0 (Sentinel-2 water pixels can be 0)

input_folder = r"C:\Users\My Pc\Documents\river project aiq\Imagery_Output\Sentinel"
output_folder = r"C:\Users\My Pc\Documents\river project aiq\Imagery_Output\Sentinel_Merged"

os.makedirs(output_folder, exist_ok=True)

# Find all tif files
all_files = glob.glob(os.path.join(input_folder, "*.tif"))

# Group tiles by river name
river_tiles = {}
for f in all_files:
    filename = os.path.basename(f)
    if '-000' in filename:
        river_name = filename.split('-000')[0]
    else:
        river_name = filename.replace('.tif', '')
    if river_name not in river_tiles:
        river_tiles[river_name] = []
    river_tiles[river_name].append(f)

print(f"Found {len(river_tiles)} rivers to merge")


def merge_tiled(valid_tiles, output_file):
    """Memory-efficient merge using tiled windowed writing."""

    # Step 1: Get bounds + metadata from all tiles
    src_files = [rasterio.open(t) for t in valid_tiles]

    # Compute overall bounds
    lefts   = [s.bounds.left   for s in src_files]
    bottoms = [s.bounds.bottom for s in src_files]
    rights  = [s.bounds.right  for s in src_files]
    tops    = [s.bounds.top    for s in src_files]

    left, bottom, right, top = min(lefts), min(bottoms), max(rights), max(tops)

    res_x = src_files[0].res[0]
    res_y = src_files[0].res[1]
    count = src_files[0].count

    width  = int(round((right - left) / res_x))
    height = int(round((top - bottom) / res_y))

    transform = rasterio.transform.from_origin(left, top, res_x, res_y)

    out_meta = src_files[0].meta.copy()
    out_meta.update({
        "driver":    "GTiff",
        "height":    height,
        "width":     width,
        "transform": transform,
        "crs":       TARGET_CRS,   # force a single CRS — fixes CRS mismatch warning
        "dtype":     "float32",
        "compress":  "lzw",
        "tiled":     True,
        "blockxsize": 512,
        "blockysize": 512,
        "nodata":    NODATA_VAL,   # tells QGIS what to treat as transparent
        "BIGTIFF":   "YES"         # allow >4GB output files
    })

    print(f"  Output size: {width} x {height} px, {count} bands")
    print(f"  Writing tiled GeoTIFF (memory-efficient)...")

    # Step 2: Write output tile by tile
    CHUNK = 2048  # rows per chunk — adjust down to 1024 if still OOM

    with rasterio.open(output_file, "w", **out_meta) as dest:
        for row_off in range(0, height, CHUNK):
            row_count = min(CHUNK, height - row_off)
            print(f"  Processing rows {row_off}–{row_off+row_count} / {height}...", end='\r')

            # Canvas filled with nodata (not zeros)
            canvas = np.full((count, row_count, width), fill_value=NODATA_VAL, dtype=np.float32)
            mask   = np.zeros((row_count, width), dtype=bool)

            chunk_top    = top - row_off * res_y
            chunk_bottom = chunk_top - row_count * res_y

            for src in src_files:
                # Check if this tile overlaps our chunk
                if src.bounds.top < chunk_bottom or src.bounds.bottom > chunk_top:
                    continue
                if src.bounds.right < left or src.bounds.left > right:
                    continue

                # Window in dest coords
                win = rasterio.windows.from_bounds(
                    max(left,         src.bounds.left),
                    max(chunk_bottom, src.bounds.bottom),
                    min(right,        src.bounds.right),
                    min(chunk_top,    src.bounds.top),
                    transform=transform
                )

                col_off_w = int(round(win.col_off))
                row_off_w = int(round(win.row_off)) - row_off
                win_w     = int(round(win.width))
                win_h     = int(round(win.height))

                if win_w <= 0 or win_h <= 0:
                    continue

                # Window in source tile coords
                src_win = rasterio.windows.from_bounds(
                    max(left,         src.bounds.left),
                    max(chunk_bottom, src.bounds.bottom),
                    min(right,        src.bounds.right),
                    min(chunk_top,    src.bounds.top),
                    transform=src.transform
                )

                try:
                    data = src.read(
                        window=src_win,
                        out_shape=(count, win_h, win_w),
                        resampling=rasterio.enums.Resampling.nearest
                    ).astype(np.float32)

                    r0 = max(0, row_off_w)
                    r1 = min(row_count, row_off_w + win_h)
                    c0 = max(0, col_off_w)
                    c1 = min(width, col_off_w + win_w)

                    dr = r1 - r0
                    dc = c1 - c0

                    if dr > 0 and dc > 0:
                        canvas[:, r0:r1, c0:c1] = np.where(
                            mask[r0:r1, c0:c1],
                            canvas[:, r0:r1, c0:c1],
                            data[:, :dr, :dc]
                        )
                        mask[r0:r1, c0:c1] = True

                except Exception as e:
                    print(f"\n  ⚠ Skipping tile overlap: {e}")

            # Write this chunk to output
            dest.write(canvas, window=rasterio.windows.Window(0, row_off, width, row_count))

    for src in src_files:
        src.close()


# ── Main loop ────────────────────────────────────────────────────────────────

for river_name, tiles in river_tiles.items():
    output_file = os.path.join(output_folder, f"{river_name}_merged.tif")

    if os.path.exists(output_file):
        print(f"⏭ Already merged: {river_name}")
        continue

    # Validate tiles
    print(f"\nValidating tiles for: {river_name}...")
    valid_tiles = []
    for t in tiles:
        try:
            with rasterio.open(t) as src:
                _ = src.meta
                valid_tiles.append(t)
                print(f"  ✓ {os.path.basename(t)}")
        except Exception:
            print(f"  ⚠ Corrupted, skipping: {os.path.basename(t)}")

    if not valid_tiles:
        print(f"  ✗ No valid tiles for {river_name}")
        continue

    print(f"\nMerging {len(valid_tiles)} tiles for: {river_name}...")
    try:
        merge_tiled(valid_tiles, output_file)
        size_gb = os.path.getsize(output_file) / (1024**3)
        print(f"\n✅ Done: {river_name}_merged.tif ({size_gb:.2f} GB)")
    except Exception as e:
        print(f"\n  ✗ Merge failed: {e}")


# ── Build statistics + overviews (makes QGIS display correctly) ──────────────

# print("\nBuilding statistics and overviews for QGIS...")
# for river_name in river_tiles:
#     output_file = os.path.join(output_folder, f"{river_name}_merged.tif")
#     if not os.path.exists(output_file):
#         continue

#     print(f"  Stats + overviews: {river_name}...")
#     try:
#         ds = gdal.Open(output_file, gdal.GA_Update)
#         if ds is None:
#             print(f"  ⚠ Could not open {river_name} with GDAL, skipping.")
#             continue

#         # Compute per-band statistics (writes .aux.xml sidecar used by QGIS)
#         for i in range(1, ds.RasterCount + 1):
#             ds.GetRasterBand(i).ComputeStatistics(False)

#         # Build overview pyramids (makes QGIS render fast at any zoom level)
#         ds.BuildOverviews("NEAREST", [2, 4, 8, 16])

#         ds = None  # flush + close
#         print(f"  ✅ Done: {river_name}")
#     except Exception as e:
#         print(f"  ⚠ Stats/overviews failed for {river_name}: {e}")

# print("Statistics and overviews done!")

print("\n==============================")
print("All rivers processed!")
print(f"Output: {output_folder}")
print("==============================")