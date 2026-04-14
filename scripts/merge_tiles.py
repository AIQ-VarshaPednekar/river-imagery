import os

# ── PROJ/GDAL conflict fix ────────────────────────────────────────────────────
# BACKGROUND: Both rasterio (via GDAL) and PostgreSQL install a copy of the
# PROJ library. Each has its own proj.db file. If PostgreSQL's PROJ is found
# first in the environment, rasterio fails with:
#   "proj: pj_obj_create: cannot find proj.db"
#
# FIX: Clear any inherited PROJ_DATA, PROJ_LIB, PROJ_DIR environment variables
# that point to PostgreSQL's PROJ. After this, GDAL/rasterio use their own
# bundled PROJ data directory (found via the rasterio wheel's search paths).
#
# WHY pop() instead of setting to pyproj's dir?
#   On some systems, clearing these vars lets GDAL use its internal PROJ
#   without conflicts. If clearing doesn't work, uncomment the pyproj
#   approach below. MUST be done before ANY rasterio, gdal, or pyproj import.
for _var in ("PROJ_DATA", "PROJ_LIB", "PROJ_DIR"):
    os.environ.pop(_var, None)   # Remove if present; no error if absent

import glob       # glob.glob() to find all .tif files in a folder with wildcards
import numpy as np  # NumPy for array operations on raster data (fill, where, etc.)
import rasterio   # Core raster I/O library: read/write GeoTIFFs
from rasterio.merge import merge            # (imported but not used — windowed merge below is used instead)
from rasterio.transform import from_bounds  # (imported but not used — from_origin is used instead)
from rasterio.warp import calculate_default_transform, reproject, Resampling as WarpResampling
# rasterio.warp — (imported but not used directly; reprojection is handled by CRS override in out_meta)


# ── Constants ──────────────────────────────────────────────────────────────────

TARGET_CRS = "EPSG:4326"
# The coordinate reference system for the output merged file.
# EPSG:4326 = WGS84 geographic coordinates (lat/lon in degrees).
# This matches what GEE exports by default (crs='EPSG:4326' in export calls).
# If tiles have mixed CRSes, this forces them all to match.

NODATA_VAL = -9999
# The "no data" sentinel value written to pixels that have no valid data.
# WHY -9999 instead of 0?
#   Sentinel-2 water pixels can legitimately have reflectance values near 0.
#   Using 0 as NoData would make open water appear as transparent/missing.
#   -9999 is safely outside the range of any valid reflectance value (0–10000).
#   QGIS reads the nodata value from the GeoTIFF metadata and treats those
#   pixels as transparent in rendering.

# ── Path overrides (injected by run_step3.py at runtime) ──────────────────────
# run_step3.py uses regex to REPLACE these two lines with the actual paths
# from config.json before executing this script. They are empty by default.
input_folder  = ""    # Where *.tif tile files are (output_base_folder/Sentinel/)
output_folder = ""    # Where merged files are written (output_base_folder/Sentinel_Merged/)

os.makedirs(output_folder, exist_ok=True)   # Create output folder if it doesn't exist yet

# ── Discover all tile files ────────────────────────────────────────────────────
# glob.glob finds all .tif files in the input folder.
# GEE produces tiles named like:
#   Ganga_sentinel-000.tif   (first tile)
#   Ganga_sentinel-001.tif   (second tile)
#   Ganga_sentinel.tif       (if river fits in one tile)
all_files = glob.glob(os.path.join(input_folder, "*.tif"))

# ── Group tiles by river name ──────────────────────────────────────────────────
# Each river may have multiple tiles (GEE splits large areas into numbered tiles).
# We group them: {'Ganga_sentinel': ['Ganga_sentinel-000.tif', 'Ganga_sentinel-001.tif'], ...}
#
# Naming convention:
#   Multi-tile: filename = "<rivername>_sentinel-000.tif"  → split on '-000'
#   Single tile: filename = "<rivername>_sentinel.tif"     → strip .tif extension
river_tiles = {}
for f in all_files:
    filename = os.path.basename(f)
    if '-000' in filename:
        # Multi-tile: group key = everything before "-000"
        river_name = filename.split('-000')[0]
    else:
        # Single tile: group key = filename without extension
        river_name = filename.replace('.tif', '')
    if river_name not in river_tiles:
        river_tiles[river_name] = []
    river_tiles[river_name].append(f)

print(f"Found {len(river_tiles)} rivers to merge")


def merge_tiled(valid_tiles, output_file):
    """Memory-efficient GeoTIFF merge using chunked windowed writing.

    PROBLEM this solves:
      The standard rasterio.merge.merge() loads ALL tiles entirely into RAM.
      For large rivers (Ganga, Brahmaputra can be 5–20 GB total tiles),
      this would need 64+ GB of RAM on a single machine.

    SOLUTION: windowed (chunked) I/O:
      We process one horizontal strip (CHUNK=2048 rows) at a time.
      For each strip:
        1. Create an in-memory "canvas" array filled with NODATA_VAL
        2. For each tile that OVERLAPS this strip (bounding box check):
           a. Compute the overlap window in both destination and source coordinates
           b. Read just that overlap window from the source tile
           c. Paste non-nodata pixels onto the canvas (first-come-first-served)
        3. Write the filled canvas strip to the output file
      The next iteration starts the next strip, discarding the previous canvas.

    MEMORY USAGE:
      ~= CHUNK_ROWS * total_width * n_bands * 4 bytes per float32
      For a 50,000-pixel wide river with 10 bands and 2048 chunk rows:
      ~= 2048 * 50000 * 10 * 4 = ~4 GB RAM per chunk
      Adjust CHUNK down (to 1024 or 512) if running out of memory.

    WHY fill canvas with NODATA_VAL (not zeros)?
      If zeros were used, areas with no tile coverage would look like valid data.
      NODATA_VAL (-9999) tells QGIS to show those pixels as transparent.

    WHY np.where for pasting?
      The `mask` array tracks which output pixels have already been filled.
      np.where(mask, old_canvas, new_data) = keep existing where already filled,
      otherwise use the new tile data. This implements first-tile-wins overlap handling.

    Args:
        valid_tiles: List of absolute paths to validated .tif tile files
        output_file: Absolute path of the merged output .tif to write
    """

    # ── Step 1: Read metadata from all tiles ────────────────────────────────────
    # We need bounds from ALL tiles to know the total output canvas dimensions.
    # We open all files here and close them at the end of the function.
    src_files = [rasterio.open(t) for t in valid_tiles]

    # Compute the bounding box that encompasses ALL tiles
    lefts   = [s.bounds.left   for s in src_files]
    bottoms = [s.bounds.bottom for s in src_files]
    rights  = [s.bounds.right  for s in src_files]
    tops    = [s.bounds.top    for s in src_files]

    # Overall bounding box: tightest rectangle containing all tiles
    left, bottom, right, top = min(lefts), min(bottoms), max(rights), max(tops)

    # Use the first tile's pixel resolution and band count as representative
    # (all tiles from the same GEE export should have identical resolution/bands)
    res_x = src_files[0].res[0]   # Pixel width in degrees (e.g. 8.98e-5 for 10m at equator)
    res_y = src_files[0].res[1]   # Pixel height in degrees
    count = src_files[0].count     # Number of bands in the imagery

    # Calculate output dimensions in pixels
    # int(round(...)) avoids floating-point rounding causing off-by-one dimension errors
    width  = int(round((right - left) / res_x))
    height = int(round((top - bottom) / res_y))

    # Geotransform: maps pixel (col=0, row=0) to geographic coordinates (left, top)
    # from_origin(left, top, pixel_width, pixel_height) creates this affine transform
    transform = rasterio.transform.from_origin(left, top, res_x, res_y)

    # Build the output file metadata (inherits most settings from a source tile)
    out_meta = src_files[0].meta.copy()
    out_meta.update({
        "driver":    "GTiff",      # Format: GeoTIFF (required for GIS)
        "height":    height,       # Output height in pixels
        "width":     width,        # Output width in pixels
        "transform": transform,    # Affine geotransform for georeferencing
        "crs":       TARGET_CRS,   # Force all tiles to same CRS (EPSG:4326)
        "dtype":     "float32",    # 32-bit float: covers Sentinel reflectance (0-10000)
                                   # and DEM elevation (any real number)
        "compress":  "lzw",        # LZW lossless compression (saves ~50-70% disk space)
        "tiled":     True,         # Internal tiling: faster random-access reads in QGIS
        "blockxsize": 512,         # Tile width for internal tiling (512x512 standard)
        "blockysize": 512,         # Tile height for internal tiling
        "nodata":    NODATA_VAL,   # Metadata tag: QGIS treats -9999 pixels as transparent
        "BIGTIFF":   "YES"         # Allow output files > 4 GB (standard GeoTIFF is limited to 4 GB)
    })

    print(f"  Output size: {width} x {height} px, {count} bands")
    print(f"  Writing tiled GeoTIFF (memory-efficient)...")

    # ── Step 2: Write output strip by strip ─────────────────────────────────────
    CHUNK = 2048   # Number of rows per processing chunk
                   # Increase for faster processing (more RAM), decrease if OOM

    with rasterio.open(output_file, "w", **out_meta) as dest:
        for row_off in range(0, height, CHUNK):
            row_count = min(CHUNK, height - row_off)   # Last chunk may be smaller
            print(f"  Processing rows {row_off}–{row_off+row_count} / {height}...", end='\r')

            # Canvas = the output array for this horizontal strip.
            # Initialised with NODATA_VAL so empty areas remain as no-data.
            # Shape: (n_bands, row_count, total_width)
            canvas = np.full((count, row_count, width), fill_value=NODATA_VAL, dtype=np.float32)

            # mask[row, col] = True if that pixel has been filled by a tile.
            # Used to implement first-tile-wins: once a pixel is filled, later tiles
            # don't overwrite it (avoids seam lines where tiles overlap).
            mask   = np.zeros((row_count, width), dtype=bool)

            # Geographic bounds of this horizontal strip
            chunk_top    = top - row_off * res_y           # Top edge of strip in lat/lon
            chunk_bottom = chunk_top - row_count * res_y   # Bottom edge of strip in lat/lon

            for src in src_files:
                # ── Quick overlap check ─────────────────────────────────────────
                # Skip tiles that don't overlap this strip in latitude
                if src.bounds.top < chunk_bottom or src.bounds.bottom > chunk_top:
                    continue
                # Skip tiles that don't overlap this strip in longitude
                if src.bounds.right < left or src.bounds.left > right:
                    continue

                # ── Compute destination window (in output pixel coordinates) ─────
                # from_bounds creates a Window object: (col_off, row_off, width, height)
                # Clamped to the intersection of the tile bounds and the strip bounds.
                win = rasterio.windows.from_bounds(
                    max(left,         src.bounds.left),     # Left edge of overlap
                    max(chunk_bottom, src.bounds.bottom),   # Bottom edge of overlap
                    min(right,        src.bounds.right),    # Right edge of overlap
                    min(chunk_top,    src.bounds.top),      # Top edge of overlap
                    transform=transform                      # Output file's geotransform
                )

                # Window pixel coordinates in the OUTPUT canvas
                col_off_w = int(round(win.col_off))            # Column offset in whole canvas
                row_off_w = int(round(win.row_off)) - row_off  # Row offset relative to THIS CHUNK
                win_w     = int(round(win.width))              # Width in pixels
                win_h     = int(round(win.height))             # Height in pixels

                if win_w <= 0 or win_h <= 0:
                    continue   # Zero-size intersection → skip

                # ── Compute source window (in SOURCE tile pixel coordinates) ─────
                # Same geographic intersection, but using the SOURCE tile's geotransform
                src_win = rasterio.windows.from_bounds(
                    max(left,         src.bounds.left),
                    max(chunk_bottom, src.bounds.bottom),
                    min(right,        src.bounds.right),
                    min(chunk_top,    src.bounds.top),
                    transform=src.transform   # SOURCE tile's geotransform
                )

                try:
                    # Read the overlap region from the source tile.
                    # out_shape forces the data to match win_h x win_w pixels
                    # (handles sub-pixel resolution differences between tiles).
                    # Resampling.nearest preserves original pixel values without interpolation.
                    data = src.read(
                        window=src_win,
                        out_shape=(count, win_h, win_w),
                        resampling=rasterio.enums.Resampling.nearest
                    ).astype(np.float32)

                    # Clamp destination indices to the canvas bounds
                    r0 = max(0, row_off_w)
                    r1 = min(row_count, row_off_w + win_h)
                    c0 = max(0, col_off_w)
                    c1 = min(width, col_off_w + win_w)

                    dr = r1 - r0   # Actual rows to write
                    dc = c1 - c0   # Actual columns to write

                    if dr > 0 and dc > 0:
                        # Paste tile data onto canvas, but only for pixels not yet filled.
                        # mask[r0:r1, c0:c1] is True where already filled → keep old canvas value.
                        # mask == False → copy from this tile's data.
                        canvas[:, r0:r1, c0:c1] = np.where(
                            mask[r0:r1, c0:c1],       # Condition: already filled?
                            canvas[:, r0:r1, c0:c1],  # True: keep existing canvas value
                            data[:, :dr, :dc]          # False: use tile data
                        )
                        mask[r0:r1, c0:c1] = True   # Mark these pixels as filled

                except Exception as e:
                    print(f"\n  ⚠ Skipping tile overlap: {e}")

            # ── Write the completed canvas strip to the output file ─────────────
            # Window specifies which rows of the output file this strip corresponds to
            dest.write(canvas, window=rasterio.windows.Window(0, row_off, width, row_count))

    # Close all source file handles
    for src in src_files:
        src.close()


# ── Main merge loop ────────────────────────────────────────────────────────────
# Iterates over all river groups discovered above and merges each one.

for river_name, tiles in river_tiles.items():
    output_file = os.path.join(output_folder, f"{river_name}_merged.tif")

    # Skip if already merged (idempotent — safe to re-run)
    if os.path.exists(output_file):
        print(f"⏭ Already merged: {river_name}")
        continue

    # ── Validate tiles before attempting merge ─────────────────────────────────
    # A corrupted tile (incomplete download, GEE error) would crash rasterio.open().
    # We pre-validate each tile and only merge the healthy ones.
    print(f"\nValidating tiles for: {river_name}...")
    valid_tiles = []
    for t in tiles:
        try:
            with rasterio.open(t) as src:
                _ = src.meta          # Accessing .meta reads the file header
                valid_tiles.append(t) # If we got here, header is valid
                print(f"  ✓ {os.path.basename(t)}")
        except Exception:
            print(f"  ⚠ Corrupted, skipping: {os.path.basename(t)}")

    if not valid_tiles:
        print(f"  ✗ No valid tiles for {river_name}")
        continue

    # ── Merge all valid tiles for this river ───────────────────────────────────
    print(f"\nMerging {len(valid_tiles)} tiles for: {river_name}...")
    try:
        merge_tiled(valid_tiles, output_file)
        size_gb = os.path.getsize(output_file) / (1024**3)
        print(f"\n✅ Done: {river_name}_merged.tif ({size_gb:.2f} GB)")
    except Exception as e:
        print(f"\n  ✗ Merge failed: {e}")

# ── Final summary ─────────────────────────────────────────────────────────────
print("\n==============================")
print("All rivers processed!")
print(f"Output: {output_folder}")
print("==============================")