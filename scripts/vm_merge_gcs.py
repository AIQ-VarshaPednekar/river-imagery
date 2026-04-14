#!/usr/bin/env python3
"""
vm_merge_gcs.py — Cloud VM GeoTIFF Tile Merger (runs on GCP Compute Engine)
=============================================================================
PURPOSE:
  Merge Sentinel-2 GeoTIFF tiles stored in Google Cloud Storage (GCS) into
  one merged GeoTIFF per river, then upload the merged file back to GCS.

  This script is designed to run INSIDE a GCP Compute Engine VM as part of
  the cloud merge workflow. It is:
    1. Uploaded to gs://<bucket>/scripts/vm_merge_gcs.py by the user
    2. Downloaded to the VM by the startup script (in main.tf / main.py VM launch)
    3. Executed by the startup script with --bucket, --input-prefix, --output-prefix args
    4. Deletes local temp files when done; then VM self-deletes

WHY run in a GCP VM instead of locally?
  Indian rivers like Ganga, Brahmaputra can have 20+ tiles totalling 50+ GB.
  Merging on a local laptop requires:
    - Sufficient RAM (we use chunked I/O, but still needs ~8–16 GB)
    - Sufficient disk (200 GB for large rivers)
    - Hours of download time
  A GCP VM (n2-highmem-4: 4 CPU, 32 GB RAM) has:
    - 200 GB SSD disk (configured in Terraform)
    - Fast network to GCS (~10 Gbps inside GCP)
    - Can complete in 20 minutes and then self-delete to stop billing

WHY gsutil CLI instead of google-cloud-storage Python library?
  On Debian 12, installing google-cloud-storage pulls in newer urllib3/requests
  that conflict with system packages, causing SSL errors. The gsutil CLI (already
  pre-installed on Debian-based GCE images) avoids this dependency conflict.

DATA FLOW:
  GCS bucket: gs://aiq-river-imagery/Sentinel/*.tif
      ↓ gsutil ls  → discover tile groups per river
      ↓ gsutil cp  → download tiles to /tmp/river_merge/input/<river>/
      ↓ merge_tiled() → merge into /tmp/river_merge/output/<river>_merged.tif
      ↓ gsutil cp  → upload merged file to gs://aiq-river-imagery/Sentinel_Merged/
      ↓ os.remove  → clean up local temp files (VM disk is small and expensive)

CALLED BY:
  The GCP VM startup script in main.tf / main.py's /api/vm/launch endpoint.
  Also can be run directly on any machine with gsutil and rasterio installed.

REQUIREMENTS:
  pip install rasterio numpy
  gsutil (pre-installed on GCE Debian images; or install via Google Cloud SDK)
"""

import argparse     # Parse CLI arguments (--bucket, --input-prefix, etc.)
import os           # File/directory operations (makedirs, path.exists, remove)
import sys          # sys.exit() to signal merge success or failure to the startup script
import subprocess   # Run gsutil CLI commands (ls, cp, stat)
import traceback    # Print full exception stack traces on failure
import numpy as np  # Array operations for the merge algorithm
import rasterio     # Read/write GeoTIFF raster files
import rasterio.windows  # Window objects for windowed (chunked) I/O
import rasterio.enums    # Resampling methods


# ── Constants ──────────────────────────────────────────────────────────────────
NODATA_VAL  = -9999        # NoData sentinel value (used for unset pixels in the canvas)
                           # Must match what gee_export.py and merge_tiles.py use
TARGET_CRS  = "EPSG:4326"  # WGS84 lat/lon — matches GEE export CRS
CHUNK_ROWS  = 2048          # Rows per chunk in the merge loop
                            # 2048 rows × ~50000px wide × 10 bands × 4 bytes ≈ 4 GB per chunk
                            # n2-highmem-4 has 32 GB RAM, so this is well within budget


def gsutil(*args) -> subprocess.CompletedProcess:
    """Run a gsutil CLI command and return the CompletedProcess result.

    WHY wrap gsutil in a function?
      gsutil is a shell CLI tool (part of Google Cloud SDK). We invoke it
      via subprocess for every GCS operation (list, download, upload, check).
      This wrapper reduces boilerplate — every call is `gsutil("ls", "gs://...")`.

    capture_output=True → stdout and stderr are captured (not printed to terminal).
    text=True → stdout/stderr are strings, not bytes.

    Returns:
        subprocess.CompletedProcess — has .returncode, .stdout, .stderr
    """
    cmd = ["gsutil"] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True)


def list_tile_groups(bucket_name: str, input_prefix: str) -> dict:
    """Discover and group all tile files in the GCS bucket input folder.

    WHAT this does:
      1. Lists all objects in gs://<bucket>/<input_prefix>/
      2. Filters to only .tif files (ignores .json, .txt, etc.)
      3. Skips files with "_merged" in the name (don't re-merge already-merged files)
      4. Groups by river name (extracted from the filename)

    GEE tile naming convention:
      Single tile:     Ganga_sentinel.tif           → river = "Ganga_sentinel"
      Multiple tiles:  Ganga_sentinel-000.tif etc.  → river split on "-" gives "Ganga_sentinel"

    WHY group by first "-" segment?
      GEE adds "-000", "-001", "-002" etc. when a river is too large for one file.
      The part before the first "-" is the base river name we want to group on.

    Args:
        bucket_name:  GCS bucket name (e.g. "aiq-river-imagery")
        input_prefix: GCS folder/prefix (e.g. "Sentinel")

    Returns:
        dict[str, list[str]] — {river_name: [blob_path1, blob_path2, ...]}
        blob_path is relative to the bucket (e.g. "Sentinel/Ganga_sentinel-000.tif")
    """
    r = gsutil("ls", f"gs://{bucket_name}/{input_prefix}/")
    if r.returncode != 0:
        raise RuntimeError(f"gsutil ls failed: {r.stderr}")

    groups: dict = {}
    for line in r.stdout.splitlines():
        line = line.strip()
        if not line.endswith(".tif"):
            continue   # Skip non-TIFF files

        name = os.path.basename(line)      # Extract filename from full GCS URI
        if "_merged" in name:
            continue   # Skip already-merged outputs (avoid re-merging)

        # Convert full "gs://bucket/..." URI to a bucket-relative path
        blob_path = line.replace(f"gs://{bucket_name}/", "")

        stem  = name[:-4]                                # Remove .tif extension
        river = stem.split("-")[0] if "-" in stem else stem   # Group key

        groups.setdefault(river, []).append(blob_path)

    return groups


def already_merged(bucket_name: str, output_prefix: str, river_name: str) -> bool:
    """Check if the merged output already exists in GCS.

    Uses `gsutil -q stat` which returns 0 if the object exists, 1 if not.
    The -q flag suppresses output (quiet mode).

    WHY check before merging?
      If the VM is restarted after a partial run, or if the user reruns,
      we don't want to re-merge rivers that already have a good merged file.
      This makes the script idempotent — safe to re-run.

    Args:
        bucket_name:   GCS bucket name
        output_prefix: GCS output folder/prefix (e.g. "Sentinel_Merged")
        river_name:    River group name key (e.g. "Ganga_sentinel")

    Returns:
        bool — True if merged file exists in GCS
    """
    r = gsutil("-q", "stat", f"gs://{bucket_name}/{output_prefix}/{river_name}_merged.tif")
    return r.returncode == 0   # 0 = exists, 1 = doesn't exist


def download_tile(bucket_name: str, blob_path: str, local_path: str):
    """Download a single tile from GCS to a local path.

    Skips download if the file already exists locally (idempotent).
    Creates parent directories if needed.

    Args:
        bucket_name: GCS bucket name
        blob_path:   Path within bucket (e.g. "Sentinel/Ganga_sentinel-000.tif")
        local_path:  Local absolute path to write to

    Raises:
        RuntimeError: if gsutil cp fails
    """
    if os.path.exists(local_path):
        return   # Skip if already downloaded (resume-friendly)

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    r = gsutil("cp", f"gs://{bucket_name}/{blob_path}", local_path)
    if r.returncode != 0:
        raise RuntimeError(f"Download failed: {r.stderr}")


def upload_merged(bucket_name: str, local_path: str, output_prefix: str, river_name: str):
    """Upload the locally-merged file back to GCS.

    Uses gsutil's parallel composite upload for large files (>150 MB).
    Composite upload splits the file into parts and uploads in parallel,
    then reassembles atomically in GCS. This is much faster than a single-stream upload.

    Args:
        bucket_name:   GCS bucket name
        local_path:    Local path of the merged .tif file
        output_prefix: GCS output folder (e.g. "Sentinel_Merged")
        river_name:    River name (used to build the output GCS object name)

    Raises:
        RuntimeError: if gsutil cp fails
    """
    dest = f"gs://{bucket_name}/{output_prefix}/{river_name}_merged.tif"
    size_mb = os.path.getsize(local_path) / (1024 ** 2)
    print(f"  Uploading {size_mb:.1f} MB -> {dest}")

    # -o GSUtil:parallel_composite_upload_threshold=150M
    #   Enables parallel composite upload for files > 150 MB.
    #   Dramatically faster than single-stream for multi-GB merged GeoTIFFs.
    r = gsutil("-o", "GSUtil:parallel_composite_upload_threshold=150M",
               "cp", local_path, dest)
    if r.returncode != 0:
        raise RuntimeError(f"Upload failed: {r.stderr}")
    print(f"  Upload complete")


def merge_tiled(valid_tiles: list, output_file: str):
    """Memory-efficient GeoTIFF merge using chunked windowed I/O.

    ALGORITHM (identical to merge_tiles.py — see that file for detailed comments):
      1. Open all source tiles, compute combined bounding box
      2. For each horizontal strip of CHUNK_ROWS rows:
         a. Fill canvas with NODATA_VAL
         b. For each tile overlapping this strip:
            - Compute dest window in canvas coordinates
            - Compute src window in tile coordinates
            - Read tile data for the overlap
            - Paste onto canvas (first-tile-wins using boolean mask)
         c. Write canvas strip to output file
      3. Close all source files

    WHY first-tile-wins?
      GEE tiles can have slight overlaps at their edges. First-tile-wins
      avoids averaging artefacts at seam lines. The result is seamless.

    Memory usage per chunk:
      CHUNK_ROWS * total_width * n_bands * 4 bytes (float32)

    Args:
        valid_tiles: List of absolute local paths to validated .tif tiles
        output_file: Absolute local path for the merged output .tif
    """
    src_files = [rasterio.open(t) for t in valid_tiles]

    # Compute combined bounding box across all tiles
    left   = min(s.bounds.left   for s in src_files)
    bottom = min(s.bounds.bottom for s in src_files)
    right  = max(s.bounds.right  for s in src_files)
    top    = max(s.bounds.top    for s in src_files)

    # Use first tile's resolution and band count as representative
    res_x  = src_files[0].res[0]
    res_y  = src_files[0].res[1]
    count  = src_files[0].count

    # Calculate output canvas dimensions in pixels
    width  = int(round((right - left)   / res_x))
    height = int(round((top   - bottom) / res_y))

    # Affine geotransform: maps (col=0, row=0) to (left, top) in geographic coords
    transform = rasterio.transform.from_origin(left, top, res_x, res_y)

    # Build output file metadata
    out_meta = src_files[0].meta.copy()
    out_meta.update({
        "driver":    "GTiff",
        "height":    height,
        "width":     width,
        "transform": transform,
        "crs":       TARGET_CRS,      # Force all tiles to EPSG:4326
        "dtype":     "float32",       # Float32 for Sentinel reflectance and DEM elevation
        "compress":  "lzw",           # Lossless compression
        "tiled":     True,            # Internal tiling for fast QGIS access
        "blockxsize": 512,
        "blockysize": 512,
        "nodata":    NODATA_VAL,      # -9999 = transparent in QGIS
        "BIGTIFF":   "YES",           # Allow output > 4 GB
    })

    print(f"  Canvas : {width} x {height} px, {count} bands")

    with rasterio.open(output_file, "w", **out_meta) as dest:
        for row_off in range(0, height, CHUNK_ROWS):
            row_count = min(CHUNK_ROWS, height - row_off)
            print(f"  Chunk {row_off // CHUNK_ROWS + 1}: rows {row_off}-{row_off + row_count}/{height}", end="\r")

            # Canvas = output array for this strip, initialised with NODATA
            canvas = np.full((count, row_count, width), fill_value=NODATA_VAL, dtype=np.float32)
            # mask = True at pixels already filled by a previous tile (first-tile-wins)
            mask   = np.zeros((row_count, width), dtype=bool)

            chunk_top    = top - row_off * res_y
            chunk_bottom = chunk_top - row_count * res_y

            for src in src_files:
                # Skip if this tile doesn't overlap the current strip
                if src.bounds.top < chunk_bottom or src.bounds.bottom > chunk_top:
                    continue
                if src.bounds.right < left or src.bounds.left > right:
                    continue

                # Window for the overlap in OUTPUT canvas coordinates
                win = rasterio.windows.from_bounds(
                    max(left,         src.bounds.left),
                    max(chunk_bottom, src.bounds.bottom),
                    min(right,        src.bounds.right),
                    min(chunk_top,    src.bounds.top),
                    transform=transform,
                )
                col_off_w = int(round(win.col_off))
                row_off_w = int(round(win.row_off)) - row_off  # Relative to THIS chunk
                win_w     = int(round(win.width))
                win_h     = int(round(win.height))
                if win_w <= 0 or win_h <= 0:
                    continue

                # Window for the overlap in SOURCE tile coordinates
                src_win = rasterio.windows.from_bounds(
                    max(left,         src.bounds.left),
                    max(chunk_bottom, src.bounds.bottom),
                    min(right,        src.bounds.right),
                    min(chunk_top,    src.bounds.top),
                    transform=src.transform,
                )

                try:
                    # Read tile data for the overlapping region
                    data = src.read(
                        window=src_win,
                        out_shape=(count, win_h, win_w),
                        resampling=rasterio.enums.Resampling.nearest,
                    ).astype(np.float32)

                    # Clamp destination indices to canvas bounds
                    r0 = max(0, row_off_w); r1 = min(row_count, row_off_w + win_h)
                    c0 = max(0, col_off_w); c1 = min(width,     col_off_w + win_w)
                    dr, dc = r1 - r0, c1 - c0

                    if dr > 0 and dc > 0:
                        # Paste: first-tile-wins (mask prevents overwriting filled pixels)
                        canvas[:, r0:r1, c0:c1] = np.where(
                            mask[r0:r1, c0:c1],         # Already filled? → keep old
                            canvas[:, r0:r1, c0:c1],
                            data[:, :dr, :dc],           # Not filled? → use this tile
                        )
                        mask[r0:r1, c0:c1] = True       # Mark as filled

                except Exception as e:
                    print(f"\n  Skipping overlap: {e}")

            # Write completed canvas strip to the output file
            dest.write(canvas, window=rasterio.windows.Window(0, row_off, width, row_count))

    print()  # Newline after \r progress
    for s in src_files:
        s.close()


def main():
    """Entry point: parse args, discover tiles in GCS, merge each river, upload results."""
    parser = argparse.ArgumentParser(
        description="Merge GCS river tiles into one GeoTIFF per river, upload back to GCS"
    )
    parser.add_argument("--bucket",        default="aiq-river-imagery",
                        help="GCS bucket name containing input tiles")
    parser.add_argument("--input-prefix",  default="Sentinel",
                        help="GCS folder/prefix containing raw tile .tif files")
    parser.add_argument("--output-prefix", default="Sentinel_Merged",
                        help="GCS folder/prefix for merged output .tif files")
    parser.add_argument("--work-dir",      default="/tmp/river_merge",
                        help="Local working directory for temp files (needs ~200 GB free)")
    parser.add_argument("--rivers",        nargs="*",
                        help="Specific river names to merge. Omit to merge all found in bucket.")
    args = parser.parse_args()

    # Create local working directories inside the VM's disk
    input_dir  = os.path.join(args.work_dir, "input")    # Downloaded tiles go here
    output_dir = os.path.join(args.work_dir, "output")   # Merged files go here before upload
    os.makedirs(input_dir,  exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("  GCS TILE MERGER - River Sentinel")
    print(f"  Bucket : {args.bucket}")
    print(f"  Input  : {args.input_prefix}/")
    print(f"  Output : {args.output_prefix}/")
    print("=" * 60)

    # ── Discover tile groups in GCS ────────────────────────────────────────────
    print("\nScanning bucket for tile groups...")
    river_groups = list_tile_groups(args.bucket, args.input_prefix)
    print(f"Found {len(river_groups)} river(s): {sorted(river_groups.keys())}")

    # Filter to user-specified rivers if --rivers is provided
    if args.rivers:
        river_groups = {r: t for r, t in river_groups.items() if r in args.rivers}
        print(f"Filtered to: {sorted(river_groups.keys())}")

    if not river_groups:
        print("\nNo tiles to process. Exiting.")
        sys.exit(0)   # Exit 0 (success) — no work to do is not an error

    # Track per-river outcomes for the final summary
    results = {"merged": [], "skipped": [], "failed": []}

    for river_name, blob_paths in sorted(river_groups.items()):
        print(f"\n{'=' * 60}")
        print(f"  RIVER : {river_name}  ({len(blob_paths)} tile(s))")

        # ── Skip if already merged in GCS ─────────────────────────────────────
        if already_merged(args.bucket, args.output_prefix, river_name):
            print(f"  Already merged in GCS - skipping")
            results["skipped"].append(river_name)
            continue

        try:
            # ── Download tiles ─────────────────────────────────────────────────
            river_dir = os.path.join(input_dir, river_name)
            os.makedirs(river_dir, exist_ok=True)

            print(f"  Downloading {len(blob_paths)} tile(s)...")
            local_tiles = []
            for blob_path in blob_paths:
                fname      = os.path.basename(blob_path)
                local_path = os.path.join(river_dir, fname)
                download_tile(args.bucket, blob_path, local_path)
                local_tiles.append(local_path)
                print(f"    OK {fname}")

            # ── Validate tiles ─────────────────────────────────────────────────
            # Attempt to open each tile's header. Corrupted downloads will fail here.
            valid_tiles = []
            for t in local_tiles:
                try:
                    with rasterio.open(t) as src:
                        _ = src.meta       # Read header → fails for corrupted files
                        valid_tiles.append(t)
                except Exception:
                    print(f"  Corrupted: {os.path.basename(t)}")

            if not valid_tiles:
                print(f"  No valid tiles for {river_name}")
                results["failed"].append(river_name)
                continue

            # ── Merge tiles into one GeoTIFF ───────────────────────────────────
            print(f"\n  Merging {len(valid_tiles)} tile(s)...")
            output_file = os.path.join(output_dir, f"{river_name}_merged.tif")
            merge_tiled(valid_tiles, output_file)

            size_gb = os.path.getsize(output_file) / (1024 ** 3)
            print(f"  Merged -> {size_gb:.2f} GB")

            # ── Upload merged file to GCS ──────────────────────────────────────
            upload_merged(args.bucket, output_file, args.output_prefix, river_name)

            # ── Clean up local temp files ──────────────────────────────────────
            # The VM has limited disk (200 GB). Delete processed tiles and merged
            # output immediately to free space for the next river.
            os.remove(output_file)
            for t in local_tiles:
                try:
                    os.remove(t)
                except OSError:
                    pass   # Non-fatal: disk cleanup failure doesn't mean merge failed

            results["merged"].append(river_name)

        except Exception as e:
            print(f"  FAILED: {e}")
            traceback.print_exc()   # Full stack trace so we can diagnose in VM logs
            results["failed"].append(river_name)

    # ── Final summary ──────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print("  FINAL SUMMARY")
    print(f"  Merged  ({len(results['merged'])}): {results['merged']}")
    print(f"  Skipped ({len(results['skipped'])}): {results['skipped']}")
    print(f"  Failed  ({len(results['failed'])}): {results['failed']}")
    print("=" * 60)

    # Exit with code 1 if any rivers failed → startup script logs the error code
    # Exit with code 0 if everything succeeded or was skipped → startup script self-deletes cleanly
    sys.exit(1 if results["failed"] else 0)


if __name__ == "__main__":
    main()
