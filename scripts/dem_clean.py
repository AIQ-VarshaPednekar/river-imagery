"""
raster_clean.py
---------------
Production-ready raster cleaning for DEM/nDSM and Sentinel-2 GeoTIFFs.

No ArcGIS license required.
Uses block/window processing — does NOT load entire raster into RAM.

Usage:
    python raster_clean.py dem    <input.tif> <output.tif>
    python raster_clean.py s2     <input.tif> <output.tif>
    python raster_clean.py both   <dem_in.tif> <dem_out.tif> <s2_in.tif> <s2_out.tif>

Requirements:
    pip install rasterio numpy

WHAT THIS FILE DOES (big picture):
------------------------------------
Satellite/drone rasters often contain garbage pixels:
  - Pixels with value 0 that mean "no data here" (not real sea level or black)
  - NaN values from failed sensor reads
  - Padded border pixels that are all-zero across every band

This script hunts those pixels down block-by-block (never loading the full
image into RAM) and stamps them with the file's official NoData value so that
GIS tools, visualisers, and ML pipelines all agree: "this pixel is invalid."

Two flavours of cleaning:
  • DEM  (single-band float32 elevation)  → clean_dem()
  • S2   (multi-band uint16 reflectance)  → clean_sentinel()
  • Auto → detect_and_clean() picks the right one by band count.
"""

import sys           # used for sys.argv (CLI args) and sys.exit() (error exits)
import argparse      # standard library for building --help-style CLIs
import numpy as np   # numerical array operations (masks, math, NaN checks)
import rasterio      # reads/writes GeoTIFF files; gives us CRS, transforms, windows
from rasterio.enums import Resampling   # enum for choosing overview resampling algorithm

# ─────────────────────────────────────────────────────────────────────────────
# DEM / nDSM CLEANING  (single band, float32)
# ─────────────────────────────────────────────────────────────────────────────

def clean_dem(input_path: str, output_path: str) -> None:
    """
    Clean a single-band float32 DEM / nDSM raster.

    WHY THIS FUNCTION EXISTS:
        A Digital Elevation Model (DEM) or normalised Digital Surface Model (nDSM)
        stores height-above-ground as floating point numbers.  Common data problems:
          1. NaN  — sensor dropout or calculation failure → meaningless
          2. 0.0  — could be real sea-level, but for urban nDSMs it almost always
                    means "no LiDAR return / no valid measurement"
          3. Negative values — physically impossible for nDSM (height above ground
                    can't be negative) and suspicious for raw DEMs

        All three are stamped with the NoData sentinel so downstream tools skip them.

    HOW IT PROCESSES WITHOUT LOADING EVERYTHING INTO RAM:
        rasterio lets us iterate over "windows" (rectangular tile sub-regions).
        We read one tile at a time, fix it, write it, then move on.
        Peak RAM usage ≈ one tile × dtype size, not full raster.

    Parameters
    ----------
    input_path  : str  Path to the source DEM GeoTIFF.
    output_path : str  Path for the cleaned output GeoTIFF.
    """
    NODATA_DEFAULT = -9999.0
    # -9999.0 is the geospatial industry's conventional "no data" sentinel for
    # float elevation rasters.  It's far outside any real elevation range, so
    # tools can reliably distinguish it from valid measurements.

    # ── Pretty-print a header so the operator knows which file is being processed ─
    print(f"\n{'─'*60}")
    print(f"  DEM CLEAN")
    print(f"  Input  : {input_path}")
    print(f"  Output : {output_path}")
    print(f"{'─'*60}")

    with rasterio.open(input_path) as src:
        # rasterio.open() memory-maps the file — the full pixel data is NOT read
        # yet; only the header (CRS, transform, dtype, shape) is parsed.

        # ── Validate ──────────────────────────────────────────────────────────
        if src.count != 1:
            # src.count = number of bands in the file.
            # DEMs must be single-band.  If someone accidentally passes a
            # Sentinel-2 file here, we fail loudly rather than silently corrupt.
            raise ValueError(
                f"Expected a single-band DEM, got {src.count} bands. "
                "Run clean_sentinel() for multi-band data."
            )
        dtype_str = src.dtypes[0]
        # src.dtypes is a tuple of dtype strings per band e.g. ('float32',).
        # We grab index [0] because we already confirmed there's exactly 1 band.

        # ── Choose the right NoData sentinel value for this dtype ─────────────
        # Different integer types can't store -9999.0 (would overflow/truncate).
        # This lookup table maps each possible dtype to a safe sentinel value.

        # Pick a NoData value valid for the actual dtype
        _nodata_defaults = {
            "uint8":   255,          # max uint8, clearly "saturated / no data"
            "uint16":  65535,        # max uint16
            "uint32":  4294967295,   # max uint32
            "int8":   -1,            # -1 is safe for signed byte
            "int16":  -9999,         # fits in int16 range (-32768 … 32767)
            "int32":  -9999,         # int32 has plenty of room
            "float32": -9999.0,      # standard float elevation sentinel
            "float64": -9999.0,
        }
        if src.nodata is not None:
            # The file already declares its own NoData value in its metadata.
            # Honour it: use the same value for both reading and writing
            # so we never accidentally treat valid pixels as NoData.
            nodata_in  = src.nodata   # value that the SOURCE uses for NoData
            nodata_out = src.nodata   # we keep the same convention in the output
        else:
            # File has no NoData declaration → pick a safe default for its dtype.
            nodata_out = _nodata_defaults.get(dtype_str, 0)
            nodata_in  = nodata_out  # nothing pre-existing to match
            # nodata_in = nodata_out here because there are no pre-existing
            # NoData pixels encoded in the file — anything that looks like
            # nodata_out was data, but we'll still mark zeros/NaN as invalid.

        # ── Log key metadata so the operator can sanity-check ─────────────────
        print(f"  CRS       : {src.crs}")       # coordinate reference system
        print(f"  Shape     : {src.height} rows × {src.width} cols")
        print(f"  NoData    : {nodata_in}")

        # ── Prepare output profile ────────────────────────────────────────────
        # ── Build the output GeoTIFF profile (metadata for the new file) ──────
        profile = src.profile.copy()
        # src.profile is a dict containing everything rasterio needs to create
        # a GeoTIFF: driver, width, height, count, dtype, crs, transform, etc.
        # .copy() so we don't mutate the source file's live metadata object.
        profile.update(
            driver     = "GTiff",     # explicitly request GeoTIFF format
            # dtype is intentionally NOT overridden → output keeps source dtype
            nodata     = nodata_out,  # embed our chosen NoData sentinel in the file header
            count      = 1,           # confirm single-band output
            compress   = "lzw",       # lossless LZW compression; keeps file small
            tiled      = True,        # write in 256×256 tiles (faster random access)
            blockxsize = 256,         # tile width in pixels
            blockysize = 256,         # tile height in pixels
            interleave = "band",      # all of band 1 stored before band 2, etc.
                                      # For 1-band files this has no practical effect,
                                      # but it's the correct explicit setting.
        )
        # Remove JPEG-only compression keys that are illegal in LZW-compressed GTiffs.
        # If the source was JPEG-compressed, these keys survive in the copied profile
        # and would cause rasterio to throw on open().
        # Remove keys that are only valid for JPEG driver
        for key in ("quality", "jpeg_quality"):
            profile.pop(key, None)      # pop(key, None) → no error if key absent

        # ── Pixel counters for the final summary stats ─────────────────────────
        total_pixels  = 0     # how many pixels we visited
        nulled_pixels = 0      # how many we stamped as NoData

        # ── Block-by-block processing ─────────────────────────────────────────
        with rasterio.open(output_path, "w", **profile) as dst:
            # Open the output file for writing NOW, before the loop.
            # rasterio allocates the file on disk with the correct dimensions
            # immediately; we fill it tile-by-tile in the loop below.

            for _, window in src.block_windows(1):
                # src.block_windows(1) yields (block_index, Window) pairs.
                # Each Window describes a rectangular sub-region of the raster
                # aligned to the on-disk tile grid of band 1.
                # We ignore the block_index (hence the _ throwaway variable)
                # because we only need the spatial Window.

                # Read only the pixels inside this tile — NOT the whole file.
                # dtype is preserved (e.g., float32 stays float32).
                # Read as float32 — preserve original dtype
                data = src.read(1, window=window)  # dtype preserved as-is
                # src.read(band_index, window=...) → 2-D numpy array (rows, cols)

                # ── Build Boolean masks — True means "this pixel is invalid" ──
                mask_nan    = np.isnan(data)
                # np.isnan() returns True wherever the float value is IEEE NaN.
                # NaN can appear from failed GEE exports, divide-by-zero in DSM
                # generation, or corrupt files.

                mask_nodata = (data == nodata_in)
                # Pixels already flagged as NoData in the source file.
                # We preserve them; they must survive the cleaning pass unchanged.

                mask_zero   = (data <= 0)
                # For nDSMs, elevation ≤ 0 means "below or at ground" which is
                # physically impossible for a surface model (a building can't be
                # underground).  For raw DEMs this is debatable (coastal areas
                # legitimately sit at 0 or slightly below MSL), hence the warning
                # printed later if > 30 % of pixels get nulled.

                # Combine all three masks with bitwise OR.
                # A pixel is bad if it triggers ANY of the three conditions.
                combined_mask = mask_nan | mask_nodata | mask_zero

                # ── Apply the mask in-place (avoids allocating a new array) ───
                np.putmask(data, combined_mask, nodata_out)
                # np.putmask(arr, mask, values):
                #   wherever mask is True → set arr[pixel] = nodata_out
                #   This modifies `data` in-place (same memory buffer), which is
                #   more memory-efficient than data[combined_mask] = nodata_out
                #   for large arrays because it avoids building an index array.

                # ── Accumulate stats for the post-run report ──────────────────
                total_pixels  += data.size      # .size = rows × cols
                nulled_pixels += int(combined_mask.sum())
                # combined_mask.sum() counts True values (each True = 1 pixel nulled)
                # int() cast because numpy returns numpy.int64; we want a plain Python int

                
                # ── Write the cleaned tile to the output file ─────────────────
                dst.write(data, 1, window=window)
                # Write band 1, for this specific window.
                # rasterio handles the byte offset arithmetic internally —
                # this tile lands in exactly the right location in the output file.

    # ── Overviews (build after closing write handle) ──────────────────────────
    # ── Build image overviews (pyramid levels) after the file is fully written ─
    # Overviews are pre-computed down-sampled versions of the raster (½, ¼, ⅛ …).
    # GIS tools like QGIS use them to render the raster quickly when zoomed out,
    # instead of reading every pixel.  They must be built AFTER the write handle
    # is closed (the `with` block above), otherwise the file is still locked.
    print("  Building overviews …")
    with rasterio.open(output_path, "r+") as dst:
        # "r+" = open for read+write (file already exists; we're appending overviews)
        dst.build_overviews([2, 4, 8, 16], Resampling.average)
        # [2, 4, 8, 16] = overview levels: ½ res, ¼ res, ⅛ res, 1/16 res
        # Resampling.average = each overview pixel = mean of the source pixels it covers
        #   (appropriate for continuous data like elevation; preserves numeric fidelity)
        dst.update_tags(ns="rio_overview", resampling="average")
        # Stores a metadata tag recording which resampling was used.
        # Some tools (e.g., GDAL) read this to know how to update overviews later

    # ── Calculate & print the nulling percentage ──────────────────────────────
    pct = 100.0 * nulled_pixels / total_pixels if total_pixels else 0.0
    # Guard against division by zero with the ternary: if total_pixels == 0 → 0.0
    print(f"  Pixels nulled  : {nulled_pixels:,} / {total_pixels:,} ({pct:.2f}%)")

    if pct > 30.0:
        # Sanity-check heuristic: nulling more than 30 % of a DEM is suspicious.
        # It usually means the mask_zero rule is being too aggressive —
        # e.g., a coastal DEM where ocean pixels are legitimately zero elevation.
        print(
            "  ⚠  WARNING: >30 % of pixels were nulled.\n"
            "     If this is a coastal/sea-level area, zero elevation may be valid.\n"
            "     Consider disabling the 'mask_zero' rule for your use case."
        )

    # ── Quick stats ───────────────────────────────────────────────────────────
    # ── Print human-readable statistics for a final quality check ─────────────
    _print_dem_stats(output_path, nodata_out)
    print(f"  ✓ Saved → {output_path}\n")


def _print_dem_stats(path: str, nodata: float) -> None:
    """
    Print min/max/mean/std for the valid pixels of a cleaned DEM.

    WHY STREAMING STATS (not np.mean() on the full array):
        A 10 m resolution DEM of India is ~500 million pixels.
        Loading it all into RAM as float64 would need ~4 GB.
        Instead we compute a running sum and sum-of-squares per tile —
        the mathematically equivalent online algorithm — using only
        one tile's worth of RAM at a time.

    Parameters
    ----------
    path   : str   Path to the already-written output GeoTIFF.
    nodata : float The NoData sentinel value (used to exclude those pixels).
    """
    # Accumulators for the online statistics calculation
    vmin = vmax = vsum = vsum2 = None
    # None signals "not yet initialised" — we can't set min to 0
    # because 0 might be less than all valid pixels.
    vcount = 0   # running count of valid pixels seen so far

    with rasterio.open(path) as src:
        for _, window in src.block_windows(1):
            # Iterate tile-by-tile (same window grid as the write pass)
            chunk = src.read(1, window=window).astype("float64")
            # Cast to float64 for the statistics arithmetic.
            # float32 can accumulate significant rounding error in large sums;
            # float64 gives us 15 significant digits which is more than enough.

            # ── Build the validity mask ────────────────────────────────────────
            valid = chunk[~np.isnan(chunk) & (chunk != float(nodata))]
            # Step by step:
            #   np.isnan(chunk)       → True where pixel is NaN
            #   ~np.isnan(chunk)      → True where pixel is NOT NaN (invert)
            #   chunk != float(nodata)→ True where pixel is NOT the NoData sentinel
            #   both conditions ANDed → True only for pixels that are real numbers
            #                           and not the sentinel
            #   chunk[...]            → fancy indexing: extract only those pixels
            #                           as a flat 1-D array

            if valid.size == 0:
                # This entire tile was all-NoData — nothing to accumulate, skip.
                continue

            # ── Update running min/max ─────────────────────────────────────────
            vmin   = float(valid.min()) if vmin is None else min(vmin, float(valid.min()))
            vmax   = float(valid.max()) if vmax is None else max(vmax, float(valid.max()))
            # On the first tile: vmin is None → just take tile's min.
            # On subsequent tiles: compare running min against this tile's min.

            # ── Update running sum and sum-of-squares ─────────────────────────
            vsum   = float(valid.sum()) + (vsum  or 0.0)
            vsum2  = float((valid ** 2).sum()) + (vsum2 or 0.0)
            # vsum:  Σ x       — used to compute mean = Σx / n
            # vsum2: Σ x²      — used with mean to compute variance:
            #                    Var = (Σx²)/n − mean²   (computational formula)
            # (vsum or 0.0) handles the None-on-first-tile case cleanly.
            vcount += valid.size        # accumulate total pixel count

    if vcount == 0:
        # All pixels were NoData — the cleaning rules were probably too aggressive
        # or the input file was already empty/corrupt.
        print("  ⚠  No valid pixels remain — check masking conditions.")
        return

    # ── Compute final statistics from accumulators ────────────────────────────
    mean     = vsum / vcount
    # Classic mean: sum of values divided by count

    variance = vsum2 / vcount - mean ** 2
    # Online variance formula: E[x²] − (E[x])²
    # Mathematically identical to np.var() but computed without storing all x.

    std      = float(variance ** 0.5) if variance >= 0 else 0.0
    # Square root of variance = standard deviation.
    # Guard against tiny negative variance due to floating-point rounding
    # (e.g., variance = -1e-12 instead of 0.0) by clamping to 0.

    print(f"\n  Output stats (valid pixels only):")
    print(f"    Min    : {vmin:.3f} m")     # 3 decimal places = mm precision
    print(f"    Max    : {vmax:.3f} m")
    print(f"    Mean   : {mean:.3f} m")
    print(f"    Std Dev: {std:.3f} m")
    print(f"    Count  : {vcount:,}")


# ─────────────────────────────────────────────────────────────────────────────
# SENTINEL-2 CLEANING  (multi-band, uint16)
# ─────────────────────────────────────────────────────────────────────────────

def clean_sentinel(input_path: str, output_path: str) -> None:
    """
    Clean a multi-band uint16 Sentinel-2 raster.

    WHY THIS FUNCTION EXISTS:
        Sentinel-2 L2A data exported from GEE arrives as uint16 reflectance
        (values roughly 0–10 000, scaled by 10 000).  Common data problems:
          1. All-zero pixels — tile-edge padding added by GEE when the acquisition
             footprint doesn't cover the full export rectangle.  Every band is 0.
          2. NaN pixels — theoretically impossible in uint16 (integers have no NaN),
             but if someone converts to float at any pipeline step, NaN can appear.

        The cleaning rule is intentionally conservative:
          → A pixel is invalid ONLY if ALL bands are zero simultaneously.
          → A pixel where just ONE band happens to be zero is LEFT ALONE
            (zero reflectance in a single band can be physically real, e.g.,
             deep water absorbs nearly all NIR).

    HOW MULTI-BAND WINDOWED I/O WORKS:
        We read ALL bands for one tile at a time → shape (bands, rows, cols).
        The all-zero mask is computed across the band axis (axis=0) → shape (rows, cols).
        Then we set those pixel locations to 0 (NoData) across every band.

    Parameters
    ----------
    input_path  : str  Path to the source Sentinel-2 GeoTIFF.
    output_path : str  Path for the cleaned output GeoTIFF.
    """
    NODATA_S2 = 0   # uint16 — 0 is the natural NoData for Sentinel-2
    # For uint16 Sentinel-2, 0 is the natural NoData: a real-world surface always
    # reflects at least some light, so reflectance = 0 means "no measurement."

    print(f"\n{'─'*60}")
    print(f"  SENTINEL-2 CLEAN")
    print(f"  Input  : {input_path}")
    print(f"  Output : {output_path}")
    print(f"{'─'*60}")

    with rasterio.open(input_path) as src:
        # ── Validate ──────────────────────────────────────────────────────────
        if src.count < 2:
            # Sentinel-2 must be multi-band. A single-band file is almost
            # certainly a DEM that was passed to the wrong function.
            raise ValueError(
                f"Expected a multi-band raster, got {src.count} band(s). "
                "Use clean_dem() for single-band DEMs."
            )
        # Cast nodata to match the actual dtype — never force int() on float rasters
        # ── Pick the correct NoData sentinel for the actual dtype ──────────────
        dtype_str = src.dtypes[0]
        # All bands in a well-formed GeoTIFF share the same dtype; [0] = band 1's dtype.
        _s2_nodata_defaults = {
            "uint8":   0,   "uint16":  0,   "uint32":  0,
            "int8":    0,   "int16":   0,   "int32":   0,
            "float32": 0.0, "float64": 0.0,
        }
        if src.nodata is not None:
            # Honour the file's declared NoData, but cast it to the correct Python type
            # so we don't accidentally store 0.0 (float) into a uint16 array.
            nodata_out = float(src.nodata) if "float" in dtype_str else int(src.nodata)
        else:
            # No declared NoData → use 0 (natural choice for uint16 reflectance)
            nodata_out = _s2_nodata_defaults.get(dtype_str, 0)
        band_count = src.count      # how many spectral bands (e.g., 12 for S2 L2A)

        print(f"  CRS       : {src.crs}")
        print(f"  Shape     : {src.height} rows × {src.width} cols × {band_count} bands")
        print(f"  NoData    : {nodata_out}")

        # ── Prepare output profile ────────────────────────────────────────────
        # ── Build output profile ───────────────────────────────────────────────
        profile = src.profile.copy()
        # Inherit ALL metadata from the source (CRS, transform, band count, dtype …)
        profile.update(
            driver     = "GTiff",
            # dtype NOT overridden — uint16 in, uint16 out (never silently change dtype)
            nodata     = nodata_out,  # stamp the NoData value into the file header
            compress   = "lzw",       # lossless compression (S2 uint16 compresses well)
            tiled      = True,
            blockxsize = 256,
            blockysize = 256,
            interleave = "band",      # BAND interleave: all of band 1, then band 2 …
                                      # (contrast with PIXEL interleave: R,G,B,R,G,B …)
                                      # BAND interleave is faster for whole-band operations.
        )
        for key in ("quality", "jpeg_quality"):
            profile.pop(key, None)      # same JPEG artefact cleanup as in clean_dem()

        total_pixels  = 0
        nulled_pixels = 0

        # ── Block-by-block processing ─────────────────────────────────────────
        with rasterio.open(output_path, "w", **profile) as dst:
            # Iterate windows using band 1 as the reference block grid
            for _, window in src.block_windows(1):
                # src.block_windows(1) uses band 1's tile grid as the reference.
                # All bands share the same spatial grid, so this is safe.

                # Read ALL bands for this tile at once.
                # data.shape = (band_count, tile_rows, tile_cols), dtype = uint16
                data = src.read(window=window)  # dtype preserved (uint16)
                # No band index argument → read() returns ALL bands.
                # This is more efficient than looping over bands individually.

                # Detect pixels where EVERY band is 0 → invalid/padded
                all_zero_mask = np.all(data == 0, axis=0)   
                # data == 0              → Boolean array, shape (bands, rows, cols)
                # np.all(..., axis=0)    → reduce along the band axis
                #                          True only if EVERY band is 0 for that pixel
                # Result shape: (rows, cols)  — one True/False per spatial pixel# shape: (rows, cols)
                

                # Detect pixels where ANY band is NaN → corrupt/missing
                # Cast to float64 to safely use np.isnan (int dtypes have no NaN)
                nan_mask = np.any(np.isnan(data.astype("float64")), axis=0)
                # uint16 cannot represent NaN (IEEE NaN is a float concept),
                # so np.isnan on a uint16 array always returns False.
                # If someone has float-ified the data upstream, we still catch NaN here.
                # .astype("float64") → safe to call isnan on
                # np.any(..., axis=0) → True if ANY band is NaN at that pixel

                combined_mask = all_zero_mask | nan_mask
                # A pixel is "bad" if it's all-zero OR contains any NaN band.

                # Apply mask in-place across every band
                data[:, combined_mask] = nodata_out
                # data[:, combined_mask]:
                #   :              → all bands (every band index)
                #   combined_mask  → Boolean 2-D index; selects all (row, col) positions
                #                    where the mask is True
                # Together: for every flagged pixel, set ALL bands to nodata_out.
                # This is a single in-place operation — no loop over bands needed.

                total_pixels  += combined_mask.size    # rows × cols for this tile 
                nulled_pixels += int(combined_mask.sum())

                # Write ALL bands for this tile back to the output file.
                dst.write(data, window=window)
                # No band index argument → writes all bands at once.

    # ── Overviews ─────────────────────────────────────────────────────────────
    # Same rationale as in clean_dem(): build after the write handle is closed.
    print("  Building overviews …")
    with rasterio.open(output_path, "r+") as dst:
        dst.build_overviews([2, 4, 8, 16], Resampling.average)
        # Resampling.average over uint16 reflectance bands is appropriate:
        # the mean of reflectance values is a physically meaningful average colour.
        dst.update_tags(ns="rio_overview", resampling="average")

    pct = 100.0 * nulled_pixels / total_pixels if total_pixels else 0.0
    print(f"  Pixels nulled  : {nulled_pixels:,} / {total_pixels:,} ({pct:.2f}%)")
    # Note: no ">30% warning" here because all-zero edge padding in S2 exports
    # is expected and can be large (up to 50% of the tile for small AOIs).

    # ── Per-band statistics for quality assurance ──────────────────────────────
    _print_sentinel_stats(output_path, nodata_out)
    print(f"  ✓ Saved → {output_path}\n")


def _print_sentinel_stats(path: str, nodata) -> None:
    """
    Print per-band min/max for valid pixels of a cleaned Sentinel-2 raster.

    WHY PER-BAND (not aggregate):
        Sentinel-2 bands have very different expected reflectance ranges.
        Band 1 (Coastal Aerosol, 443 nm) typically peaks around 2 000,
        while Band 8 (NIR, 842 nm) can exceed 8 000 over bright surfaces.
        Showing per-band stats makes it easy to spot a band that's all-zero
        (problem) or suspiciously saturated (also a problem).

    Parameters
    ----------
    path   : str  Path to the cleaned output file.
    nodata :      The NoData sentinel value (any numeric type).
    """
    with rasterio.open(path) as src:
        # Re-read nodata from the file's own metadata.
        # This guarantees we use whatever was actually written to the header,
        # not the local variable that may differ due to float/int casting above.
        _nodata = src.nodata if src.nodata is not None else nodata
        print(f"\n  Output stats per band (valid pixels only):")
        for b in range(1, src.count + 1):
            # Loop from band 1 to band N (rasterio uses 1-based band indices)
            bmin = bmax = None      # per-band running min/max; None = "not yet seen"
            for _, window in src.block_windows(b):
                # Use band b's own block grid — ensures windows align with
                # how that band's tiles are laid out on disk.
                chunk = src.read(b, window=window).astype("float64")
                # Cast to float64 so np.isnan works and arithmetic is precise.

                # ── Build per-tile validity mask ───────────────────────────────
                nan_mask  = np.isnan(chunk)
                # Identify NaN pixels (possible only if the source was float)
                if _nodata is not None and not (isinstance(_nodata, float) and np.isnan(_nodata)):
                    # Guard: if the NoData value itself is NaN (unusual), we can't use ==.
                    # np.nan == np.nan is False in Python (IEEE 754 rules).
                    # So we only build nd_mask when NoData is a real number.
                    nd_mask = (chunk == float(_nodata))
                else:
                    # NoData is NaN itself → all NaN pixels are already caught by nan_mask.
                    # Set nd_mask to all-False so we don't double-count or error.
                    nd_mask = np.zeros(chunk.shape, dtype=bool)

                # valid pixels = NOT NaN AND NOT NoData sentinel
                valid = chunk[~nan_mask & ~nd_mask]
                if valid.size == 0:
                    # Entire tile for this band was invalid — skip accumulation.
                    continue
                cmin = float(valid.min())
                cmax = float(valid.max())
                # Update running min/max (same None-guard pattern as _print_dem_stats)
                bmin = cmin if bmin is None else min(bmin, cmin)
                bmax = cmax if bmax is None else max(bmax, cmax)
            if bmin is None:
                # bmin was never updated → every tile for this band was all-NoData
                print(f"    Band {b:>2} : no valid pixels")
                # ">2" right-aligns the band number in a 2-char field (e.g., " 1", "12")
            else:
                print(f"    Band {b:>2} : min={bmin:>12.4f}  max={bmax:>12.4f}")
                # ">12.4f" right-aligns the float in 12 chars with 4 decimal places —
                # keeps all band rows visually aligned in a column.


# ─────────────────────────────────────────────────────────────────────────────
# AUTO-DETECT
# ─────────────────────────────────────────────────────────────────────────────

def detect_and_clean(input_path: str, output_path: str) -> None:
    """
    Inspect the input raster's band count and dispatch to the right cleaner.

    WHY THIS EXISTS:
        Dashboard scripts or automated pipelines may not know in advance
        whether they're processing a DEM or a Sentinel-2 image.
        Rather than forcing the caller to know, this function auto-routes
        based on the one reliable distinguisher: band count.
          • 1 band  → must be a DEM / nDSM
          • N bands → must be multi-spectral (Sentinel-2 or similar)

    WHY BAND COUNT AND NOT DTYPE:
        dtype is unreliable for routing because:
          - Some DEMs are stored as uint16 (integer elevation in dm or cm)
          - Some Sentinel-2 products are delivered as float32
        Band count, however, is structurally fixed by the sensor:
          - A DEM always has 1 band (elevation).
          - Sentinel-2 always has ≥ 4 bands.

    Parameters
    ----------
    input_path  : str  Path to the input GeoTIFF (unknown type).
    output_path : str  Path for the cleaned output GeoTIFF.
    """
    try:
        with rasterio.open(input_path) as src:
            bands = src.count   # number of bands in the file
            dtype = src.dtypes[0]   # data type of the first band
    except Exception as e:
        # rasterio raises various exceptions for corrupt/missing files.
        # We catch all of them and convert to a clean user-facing error message.
        print(f"❌ Cannot open input file: {e}")
        sys.exit(1)     # exit with non-zero code to signal failure to the shell/dashboard

    print(f"  Auto-detected: {bands} band(s), dtype={dtype}")
    # Log what we found so the operator can verify the routing decision.

    if bands == 1:
        clean_dem(input_path, output_path)      # single-band → DEM cleaner
    else:
        clean_sentinel(input_path, output_path) # multi-band → Sentinel-2 cleaner


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    """
    Construct the argparse CLI definition.

    WHY A SEPARATE FUNCTION (not inline in __main__):
        Keeps __main__ clean and makes the parser independently testable.
        Unit tests can call _build_parser() and exercise parse_args() without
        actually running any raster processing.

    Returns
    -------
    argparse.ArgumentParser
        Configured with three sub-commands: dem, s2, both.
    """
    p = argparse.ArgumentParser(
        prog="raster_clean",
        description="Block-wise DEM and Sentinel-2 GeoTIFF cleaner.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        # RawDescriptionHelpFormatter preserves newlines/indentation in the epilog
        # string exactly as written — important for the usage examples below.
        epilog="""
AUTO MODE (no mode word needed — dashboard friendly):
  python raster_clean.py input.tif output_clean.tif

EXPLICIT MODE (optional, for clarity):
  python raster_clean.py dem   input_dem.tif  output_dem_clean.tif
  python raster_clean.py s2    input_s2.tif   output_s2_clean.tif
  python raster_clean.py both  dem_in.tif dem_out.tif s2_in.tif s2_out.tif
        """,
    )

    # add_subparsers creates a "mode" positional argument whose value selects
    # which sub-command's argument rules to apply.
    sub = p.add_subparsers(dest="mode", metavar="mode")
    # dest="mode" → after parsing, the chosen sub-command is in args.mode
    # metavar="mode" → displayed as "mode" in --help output (not the internal dest name)

    # auto positional (no sub-command) handled separately in __main__
    # dem sub-command
    p_dem = sub.add_parser("dem",  help="Clean a DEM/nDSM raster (single-band float32)")
    p_dem.add_argument("input",  help="Input DEM GeoTIFF")
    p_dem.add_argument("output", help="Output cleaned GeoTIFF")

    # s2 sub-command
    p_s2  = sub.add_parser("s2",   help="Clean a Sentinel-2 raster (multi-band uint16)")
    p_s2.add_argument("input",  help="Input Sentinel-2 GeoTIFF")
    p_s2.add_argument("output", help="Output cleaned GeoTIFF")

    # both sub-command
    p_both = sub.add_parser("both", help="Clean DEM and Sentinel-2 in one call")
    p_both.add_argument("dem_input",  help="Input DEM GeoTIFF")
    p_both.add_argument("dem_output", help="Output cleaned DEM GeoTIFF")
    p_both.add_argument("s2_input",   help="Input Sentinel-2 GeoTIFF")
    p_both.add_argument("s2_output",  help="Output cleaned Sentinel-2 GeoTIFF")

    return p


if __name__ == "__main__":
    # ── Handle auto mode: exactly 2 positional args, no sub-command keyword ──
    # This is what dashboards call:  python raster_clean.py <input> <output>
    # This block runs ONLY when the script is executed directly:
    #   python raster_clean.py ...
    # It does NOT run when the file is imported as a module in another script,
    # which protects against accidentally triggering processing on import.

    # ── Handle AUTO mode (no sub-command keyword) ──────────────────────────────
    # Auto mode: user types:  python raster_clean.py input.tif output.tif
    # sys.argv = ['raster_clean.py', 'input.tif', 'output.tif']  → len = 3
    #
    # We must intercept this BEFORE argparse, because argparse would treat
    # 'input.tif' as an unknown sub-command and print an error.
    known_modes = {"dem", "s2", "both", "-h", "--help"}
    if len(sys.argv) == 3 and sys.argv[1] not in known_modes:
        # Exactly 2 user-provided arguments AND the first isn't a known sub-command.
        # This is the "just give me two filenames and auto-detect" path.
        detect_and_clean(sys.argv[1], sys.argv[2])
        sys.exit(0)

    # ── Explicit sub-command mode ─────────────────────────────────────────────
    parser = _build_parser()
    args = parser.parse_args()
    # parse_args() reads sys.argv[1:], matches sub-command, and populates args.*
    # e.g., `python raster_clean.py dem in.tif out.tif` → args.mode="dem",
    #         args.input="in.tif", args.output="out.tif"

    if args.mode is None:
        # User typed just `python raster_clean.py` with no arguments at all.
        # Print the full --help text so they know what to do.
        parser.print_help()
        sys.exit(0)

    if args.mode == "dem":
        clean_dem(args.input, args.output)

    elif args.mode == "s2":
        clean_sentinel(args.input, args.output)

    elif args.mode == "both":
        # Run DEM cleaning first, then S2 cleaning.
        # Order doesn't matter logically (they're independent files),
        # but DEM-first is conventional since DEM processing is faster
        # and gives the operator early feedback before the heavier S2 run.
        clean_dem(args.dem_input, args.dem_output)
        clean_sentinel(args.s2_input, args.s2_output)