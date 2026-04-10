#!/usr/bin/env python3
"""
vm_merge_gcs.py - River Tile Merger (runs on GCP VM)
Uses gsutil CLI instead of google-cloud-storage Python library
to avoid SSL/urllib3 conflicts on Debian 12.
"""

import argparse
import os
import sys
import subprocess
import traceback
import numpy as np
import rasterio
import rasterio.windows
import rasterio.enums

NODATA_VAL  = -9999
TARGET_CRS  = "EPSG:4326"
CHUNK_ROWS  = 2048


def gsutil(*args) -> subprocess.CompletedProcess:
    cmd = ["gsutil"] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True)


def list_tile_groups(bucket_name: str, input_prefix: str) -> dict:
    r = gsutil("ls", f"gs://{bucket_name}/{input_prefix}/")
    if r.returncode != 0:
        raise RuntimeError(f"gsutil ls failed: {r.stderr}")

    groups: dict = {}
    for line in r.stdout.splitlines():
        line = line.strip()
        if not line.endswith(".tif"):
            continue
        name = os.path.basename(line)
        if "_merged" in name:
            continue
        blob_path = line.replace(f"gs://{bucket_name}/", "")
        stem  = name[:-4]
        river = stem.split("-")[0] if "-" in stem else stem
        groups.setdefault(river, []).append(blob_path)

    return groups


def already_merged(bucket_name: str, output_prefix: str, river_name: str) -> bool:
    r = gsutil("-q", "stat", f"gs://{bucket_name}/{output_prefix}/{river_name}_merged.tif")
    return r.returncode == 0


def download_tile(bucket_name: str, blob_path: str, local_path: str):
    if os.path.exists(local_path):
        return
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    r = gsutil("cp", f"gs://{bucket_name}/{blob_path}", local_path)
    if r.returncode != 0:
        raise RuntimeError(f"Download failed: {r.stderr}")


def upload_merged(bucket_name: str, local_path: str, output_prefix: str, river_name: str):
    dest = f"gs://{bucket_name}/{output_prefix}/{river_name}_merged.tif"
    size_mb = os.path.getsize(local_path) / (1024 ** 2)
    print(f"  Uploading {size_mb:.1f} MB -> {dest}")
    r = gsutil("-o", "GSUtil:parallel_composite_upload_threshold=150M",
               "cp", local_path, dest)
    if r.returncode != 0:
        raise RuntimeError(f"Upload failed: {r.stderr}")
    print(f"  Upload complete")


def merge_tiled(valid_tiles: list, output_file: str):
    src_files = [rasterio.open(t) for t in valid_tiles]

    left   = min(s.bounds.left   for s in src_files)
    bottom = min(s.bounds.bottom for s in src_files)
    right  = max(s.bounds.right  for s in src_files)
    top    = max(s.bounds.top    for s in src_files)

    res_x  = src_files[0].res[0]
    res_y  = src_files[0].res[1]
    count  = src_files[0].count
    width  = int(round((right - left)   / res_x))
    height = int(round((top   - bottom) / res_y))

    transform = rasterio.transform.from_origin(left, top, res_x, res_y)

    out_meta = src_files[0].meta.copy()
    out_meta.update({
        "driver":    "GTiff",
        "height":    height,
        "width":     width,
        "transform": transform,
        "crs":       TARGET_CRS,
        "dtype":     "float32",
        "compress":  "lzw",
        "tiled":     True,
        "blockxsize": 512,
        "blockysize": 512,
        "nodata":    NODATA_VAL,
        "BIGTIFF":   "YES",
    })

    print(f"  Canvas : {width} x {height} px, {count} bands")

    with rasterio.open(output_file, "w", **out_meta) as dest:
        for row_off in range(0, height, CHUNK_ROWS):
            row_count = min(CHUNK_ROWS, height - row_off)
            print(f"  Chunk {row_off // CHUNK_ROWS + 1}: rows {row_off}-{row_off + row_count}/{height}", end="\r")

            canvas = np.full((count, row_count, width), fill_value=NODATA_VAL, dtype=np.float32)
            mask   = np.zeros((row_count, width), dtype=bool)

            chunk_top    = top - row_off * res_y
            chunk_bottom = chunk_top - row_count * res_y

            for src in src_files:
                if src.bounds.top < chunk_bottom or src.bounds.bottom > chunk_top:
                    continue
                if src.bounds.right < left or src.bounds.left > right:
                    continue

                win = rasterio.windows.from_bounds(
                    max(left,         src.bounds.left),
                    max(chunk_bottom, src.bounds.bottom),
                    min(right,        src.bounds.right),
                    min(chunk_top,    src.bounds.top),
                    transform=transform,
                )
                col_off_w = int(round(win.col_off))
                row_off_w = int(round(win.row_off)) - row_off
                win_w     = int(round(win.width))
                win_h     = int(round(win.height))
                if win_w <= 0 or win_h <= 0:
                    continue

                src_win = rasterio.windows.from_bounds(
                    max(left,         src.bounds.left),
                    max(chunk_bottom, src.bounds.bottom),
                    min(right,        src.bounds.right),
                    min(chunk_top,    src.bounds.top),
                    transform=src.transform,
                )

                try:
                    data = src.read(
                        window=src_win,
                        out_shape=(count, win_h, win_w),
                        resampling=rasterio.enums.Resampling.nearest,
                    ).astype(np.float32)

                    r0 = max(0, row_off_w); r1 = min(row_count, row_off_w + win_h)
                    c0 = max(0, col_off_w); c1 = min(width,     col_off_w + win_w)
                    dr, dc = r1 - r0, c1 - c0

                    if dr > 0 and dc > 0:
                        canvas[:, r0:r1, c0:c1] = np.where(
                            mask[r0:r1, c0:c1],
                            canvas[:, r0:r1, c0:c1],
                            data[:, :dr, :dc],
                        )
                        mask[r0:r1, c0:c1] = True

                except Exception as e:
                    print(f"\n  Skipping overlap: {e}")

            dest.write(canvas, window=rasterio.windows.Window(0, row_off, width, row_count))

    print()
    for s in src_files:
        s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket",        default="aiq-river-imagery")
    parser.add_argument("--input-prefix",  default="Sentinel")
    parser.add_argument("--output-prefix", default="Sentinel_Merged")
    parser.add_argument("--work-dir",      default="/tmp/river_merge")
    parser.add_argument("--rivers",        nargs="*")
    args = parser.parse_args()

    input_dir  = os.path.join(args.work_dir, "input")
    output_dir = os.path.join(args.work_dir, "output")
    os.makedirs(input_dir,  exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("  GCS TILE MERGER - River Sentinel")
    print(f"  Bucket : {args.bucket}")
    print(f"  Input  : {args.input_prefix}/")
    print(f"  Output : {args.output_prefix}/")
    print("=" * 60)

    print("\nScanning bucket for tile groups...")
    river_groups = list_tile_groups(args.bucket, args.input_prefix)
    print(f"Found {len(river_groups)} river(s): {sorted(river_groups.keys())}")

    if args.rivers:
        river_groups = {r: t for r, t in river_groups.items() if r in args.rivers}
        print(f"Filtered to: {sorted(river_groups.keys())}")

    if not river_groups:
        print("\nNo tiles to process. Exiting.")
        sys.exit(0)

    results = {"merged": [], "skipped": [], "failed": []}

    for river_name, blob_paths in sorted(river_groups.items()):
        print(f"\n{'=' * 60}")
        print(f"  RIVER : {river_name}  ({len(blob_paths)} tile(s))")

        if already_merged(args.bucket, args.output_prefix, river_name):
            print(f"  Already merged in GCS - skipping")
            results["skipped"].append(river_name)
            continue

        try:
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

            valid_tiles = []
            for t in local_tiles:
                try:
                    with rasterio.open(t) as src:
                        _ = src.meta
                        valid_tiles.append(t)
                except Exception:
                    print(f"  Corrupted: {os.path.basename(t)}")

            if not valid_tiles:
                print(f"  No valid tiles for {river_name}")
                results["failed"].append(river_name)
                continue

            print(f"\n  Merging {len(valid_tiles)} tile(s)...")
            output_file = os.path.join(output_dir, f"{river_name}_merged.tif")
            merge_tiled(valid_tiles, output_file)

            size_gb = os.path.getsize(output_file) / (1024 ** 3)
            print(f"  Merged -> {size_gb:.2f} GB")

            upload_merged(args.bucket, output_file, args.output_prefix, river_name)

            os.remove(output_file)
            for t in local_tiles:
                try:
                    os.remove(t)
                except OSError:
                    pass

            results["merged"].append(river_name)

        except Exception as e:
            print(f"  FAILED: {e}")
            traceback.print_exc()
            results["failed"].append(river_name)

    print(f"\n{'=' * 60}")
    print("  FINAL SUMMARY")
    print(f"  Merged  ({len(results['merged'])}): {results['merged']}")
    print(f"  Skipped ({len(results['skipped'])}): {results['skipped']}")
    print(f"  Failed  ({len(results['failed'])}): {results['failed']}")
    print("=" * 60)

    sys.exit(1 if results["failed"] else 0)


if __name__ == "__main__":
    main()
