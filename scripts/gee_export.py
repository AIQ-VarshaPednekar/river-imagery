"""
GEE Export Script — Sentinel-2 + SRTM DEM
==========================================
Exports satellite imagery for selected rivers to Google Drive and/or
Google Cloud Storage bucket (aiq-river-imagery).

Config is injected by run_step1.py — do not run this script directly.
"""

import ee
import geopandas as gpd
import json
import os
import time
from datetime import datetime

# =============================================================================
# CONFIGURATION — overridden at runtime by run_step1.py
# =============================================================================

SHAPEFILE_PATH       = ""
SPECIFIC_RIVERS      = []
OUTPUT_BASE_FOLDER   = ""
SENTINEL_SUBFOLDER   = "Sentinel"
DEM_SUBFOLDER        = "DEM"
BUFFER_DISTANCE      = 10000
RESOLUTION           = 10
START_DATE           = "2025-01-01"
END_DATE             = "2025-12-31"
MAX_CLOUD_COVER      = 10
DRIVE_FOLDER         = "River_Imagery_Batch"
GEE_PROJECT          = "plucky-sight-423703-k5"
MAX_CONCURRENT_TASKS = 100
SKIP_EXISTING        = True
SAVE_GEOMETRIES_ONLY = False
PROCESS_BATCH_NUMBER = None

# Band selection — overridden from config
SELECTED_BANDS = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B8A', 'B11', 'B12']

# Export target — "drive" | "gcs" | "both"
EXPORT_TARGET = "drive"

# GCS bucket (hardcoded)
GCS_BUCKET = "aiq-river-imagery"

# =============================================================================
# FUNCTIONS — GEOMETRY
# =============================================================================

def create_buffered_geometries(gdf, buffer_distance_m):
    print(f"\nCreating {buffer_distance_m}m buffers for {len(gdf)} rivers...")
    start = time.time()

    gdf_proj     = gdf.to_crs("EPSG:3857")
    gdf_buffered = gdf_proj.copy()
    gdf_buffered['geometry'] = gdf_proj.geometry.buffer(buffer_distance_m)
    gdf_buffered['area_km2'] = gdf_buffered.geometry.area / 1e6
    gdf_buffered = gdf_buffered.to_crs("EPSG:4326")

    print(f"  Done in {time.time()-start:.1f}s — total area: {gdf_buffered['area_km2'].sum():,.0f} km²")
    return gdf_buffered


def gdf_row_to_ee_geometry(row):
    geojson = json.loads(gpd.GeoSeries([row.geometry]).to_json())
    return ee.Geometry(geojson['features'][0]['geometry'])


def sanitize_filename(name):
    for ch in '<>:"/\\|?*':
        name = name.replace(ch, '_')
    return name.strip(' .')


# =============================================================================
# FUNCTIONS — GEE AUTH + TASK MANAGEMENT
# =============================================================================

def authenticate_gee():
    print("\nAuthenticating with Google Earth Engine...")
    try:
        ee.Initialize(project=GEE_PROJECT)
        print("  ✓ Authenticated")
    except Exception as e:
        print(f"  Default auth failed: {e} — attempting interactive...")
        ee.Authenticate()
        ee.Initialize(project=GEE_PROJECT)
        print("  ✓ Authenticated (interactive)")


def get_running_task_count():
    tasks = ee.batch.Task.list()
    return sum(1 for t in tasks if t.status().get('state') in ('RUNNING', 'READY'))


def wait_for_task_slots(max_concurrent):
    while True:
        running = get_running_task_count()
        if running < max_concurrent:
            return
        print(f"    ⏳ {running} tasks running, waiting for slots...", end='\r')
        time.sleep(10)


def check_existing_files(river_name):
    safe = sanitize_filename(river_name)
    s = os.path.join(OUTPUT_BASE_FOLDER, SENTINEL_SUBFOLDER, f"{safe}_sentinel.tif")
    d = os.path.join(OUTPUT_BASE_FOLDER, DEM_SUBFOLDER,      f"{safe}_dem.tif")
    return os.path.exists(s), os.path.exists(d)


# =============================================================================
# FUNCTIONS — SENTINEL-2 EXPORT
# =============================================================================

def _build_sentinel_image(ee_geometry):
    """Shared: filter collection, composite, select bands, clip."""
    collection = (
        ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
        .filterBounds(ee_geometry)
        .filterDate(START_DATE, END_DATE)
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', MAX_CLOUD_COVER))
    )
    count = collection.size().getInfo()
    if count == 0:
        return None, 0
    image = collection.median().select(SELECTED_BANDS).clip(ee_geometry)
    return image, count


def export_sentinel_to_drive(ee_geometry, river_name):
    safe     = sanitize_filename(river_name)
    filename = f"{safe}_sentinel"

    image, count = _build_sentinel_image(ee_geometry)
    if image is None:
        return None, filename, "No images found"

    task = ee.batch.Export.image.toDrive(
        image=image,
        description=filename,
        folder=f"{DRIVE_FOLDER}/Sentinel",
        fileNamePrefix=filename,
        region=ee_geometry,
        scale=RESOLUTION,
        crs='EPSG:4326',
        maxPixels=1e13,
        fileFormat='GeoTIFF',
    )
    task.start()
    return task, filename, f"{count} images"


def export_sentinel_to_gcs(ee_geometry, river_name):
    safe     = sanitize_filename(river_name)
    filename = f"{safe}_sentinel"

    image, count = _build_sentinel_image(ee_geometry)
    if image is None:
        return None, filename, "No images found"

    task = ee.batch.Export.image.toCloudStorage(
        image=image,
        description=f"{filename}_gcs",
        bucket=GCS_BUCKET,
        fileNamePrefix=f"Sentinel/{filename}",
        region=ee_geometry,
        scale=RESOLUTION,
        crs='EPSG:4326',
        maxPixels=1e13,
        fileFormat='GeoTIFF',
    )
    task.start()
    return task, filename, f"{count} images"


# =============================================================================
# FUNCTIONS — DEM EXPORT
# =============================================================================

def _build_dem_image(ee_geometry):
    """Shared: load SRTM elevation band, clip."""
    return (
        ee.Image('USGS/SRTMGL1_003')
        .select('elevation')
        .clip(ee_geometry)
    )


def export_dem_to_drive(ee_geometry, river_name):
    safe     = sanitize_filename(river_name)
    filename = f"{safe}_dem"

    task = ee.batch.Export.image.toDrive(
        image=_build_dem_image(ee_geometry),
        description=filename,
        folder=f"{DRIVE_FOLDER}/DEM",
        fileNamePrefix=filename,
        region=ee_geometry,
        scale=RESOLUTION,
        crs='EPSG:4326',
        maxPixels=1e13,
        fileFormat='GeoTIFF',
    )
    task.start()
    return task, filename


def export_dem_to_gcs(ee_geometry, river_name):
    safe     = sanitize_filename(river_name)
    filename = f"{safe}_dem"

    task = ee.batch.Export.image.toCloudStorage(
        image=_build_dem_image(ee_geometry),
        description=f"{filename}_gcs",
        bucket=GCS_BUCKET,
        fileNamePrefix=f"DEM/{filename}",
        region=ee_geometry,
        scale=RESOLUTION,
        crs='EPSG:4326',
        maxPixels=1e13,
        fileFormat='GeoTIFF',
    )
    task.start()
    return task, filename


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "="*70)
    print("   GEE BATCH RIVER IMAGERY EXPORT")
    print("="*70)
    print(f"\n  Rivers       : {len(SPECIFIC_RIVERS)}")
    print(f"  Bands        : {SELECTED_BANDS}")
    print(f"  Export target: {EXPORT_TARGET.upper()}")
    if EXPORT_TARGET in ("gcs", "both"):
        print(f"  GCS bucket   : {GCS_BUCKET}")
    print(f"  Buffer       : {BUFFER_DISTANCE}m")
    print(f"  Resolution   : {RESOLUTION}m")
    print(f"  Date range   : {START_DATE} → {END_DATE}")
    print(f"  Max cloud    : {MAX_CLOUD_COVER}%")
    print(f"  Drive folder : {DRIVE_FOLDER}")
    print(f"  Skip existing: {SKIP_EXISTING}")

    # Load shapefile
    if not os.path.exists(SHAPEFILE_PATH):
        raise FileNotFoundError(f"Shapefile not found: {SHAPEFILE_PATH}")

    all_rivers_gdf = gpd.read_file(SHAPEFILE_PATH)
    print(f"\n  Loaded {len(all_rivers_gdf)} rivers from shapefile")
    print(f"  Columns: {list(all_rivers_gdf.columns)}")

    # Find name column
    name_col = None
    for col in ("name", "NAME", "rivname", "RIVNAME", "RiverName", "River_Name", "river_name"):
        if col in all_rivers_gdf.columns:
            name_col = col
            break
    if name_col is None:
        raise ValueError(f"No river name column found. Available: {list(all_rivers_gdf.columns)}")
    if name_col != "name":
        all_rivers_gdf = all_rivers_gdf.rename(columns={name_col: "name"})

    all_rivers_gdf = all_rivers_gdf[["name", "geometry"]]

    # Filter to selected rivers
    if SPECIFIC_RIVERS:
        all_rivers_gdf = all_rivers_gdf[all_rivers_gdf['name'].isin(SPECIFIC_RIVERS)]
        print(f"  Filtered to {len(all_rivers_gdf)} selected rivers")
        missing = set(SPECIFIC_RIVERS) - set(all_rivers_gdf['name'])
        if missing:
            print(f"  ⚠ Not found in shapefile: {missing}")

    if all_rivers_gdf.empty:
        print("ERROR: No rivers to process after filtering.")
        return

    # Create output folders
    os.makedirs(os.path.join(OUTPUT_BASE_FOLDER, SENTINEL_SUBFOLDER), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_BASE_FOLDER, DEM_SUBFOLDER),      exist_ok=True)

    buffered_gdf = create_buffered_geometries(all_rivers_gdf, BUFFER_DISTANCE)

    authenticate_gee()

    results = {
        'success':        [],
        'failed':         [],
        'skipped':        [],
        'no_images':      [],
        'sentinel_tasks': [],
        'dem_tasks':      [],
    }

    total      = len(buffered_gdf)
    start_time = time.time()

    print(f"\n{'='*70}")
    print("SUBMITTING EXPORT TASKS")
    print("="*70)

    for idx, row in buffered_gdf.iterrows():
        river_name = row['name']
        i = list(buffered_gdf.index).index(idx) + 1
        print(f"\n[{i}/{total}] {river_name}  (area: {row.get('area_km2', 0):.1f} km²)")

        if SKIP_EXISTING:
            s_exists, d_exists = check_existing_files(river_name)
            if s_exists and d_exists:
                print(f"  ⏭  Skipped (local files already exist)")
                results['skipped'].append(river_name)
                continue

        try:
            wait_for_task_slots(MAX_CONCURRENT_TASKS)
            ee_geom = gdf_row_to_ee_geometry(row)

            # ── Sentinel ─────────────────────────────────────────────────────
            sentinel_ok = False

            if EXPORT_TARGET in ("drive", "both"):
                t, fname, info = export_sentinel_to_drive(ee_geom, river_name)
                if t:
                    print(f"  ✓ Sentinel → Drive  ({info})")
                    results['sentinel_tasks'].append(fname)
                    sentinel_ok = True
                else:
                    print(f"  ⚠ Sentinel Drive: {info}")

            if EXPORT_TARGET in ("gcs", "both"):
                t, fname, info = export_sentinel_to_gcs(ee_geom, river_name)
                if t:
                    print(f"  ✓ Sentinel → GCS    ({info})")
                    results['sentinel_tasks'].append(f"{fname}_gcs")
                    sentinel_ok = True
                else:
                    print(f"  ⚠ Sentinel GCS: {info}")

            if not sentinel_ok:
                results['no_images'].append(river_name)

            # ── DEM ──────────────────────────────────────────────────────────
            if EXPORT_TARGET in ("drive", "both"):
                _, fname = export_dem_to_drive(ee_geom, river_name)
                print(f"  ✓ DEM → Drive")
                results['dem_tasks'].append(fname)

            if EXPORT_TARGET in ("gcs", "both"):
                _, fname = export_dem_to_gcs(ee_geom, river_name)
                print(f"  ✓ DEM → GCS")
                results['dem_tasks'].append(f"{fname}_gcs")

            results['success'].append(river_name)

        except Exception as e:
            print(f"  ✗ Error: {e}")
            results['failed'].append((river_name, str(e)))

    elapsed = time.time() - start_time

    # Summary
    print(f"\n{'='*70}")
    print("EXPORT SUMMARY")
    print("="*70)
    print(f"  Total    : {total}")
    print(f"  ✓ Success : {len(results['success'])}")
    print(f"  ✗ Failed  : {len(results['failed'])}")
    print(f"  ⏭ Skipped : {len(results['skipped'])}")
    print(f"  ⚠ No imgs : {len(results['no_images'])}")
    print(f"  Tasks sent: {len(results['sentinel_tasks'])} sentinel, {len(results['dem_tasks'])} DEM")
    print(f"  Time      : {elapsed/60:.1f} min")

    if results['failed']:
        print("\n  Failed rivers:")
        for r, e in results['failed']:
            print(f"    - {r}: {e}")

    if results['no_images']:
        print("\n  No Sentinel images found for:")
        for r in results['no_images']:
            print(f"    - {r}")

    print(f"\n  Monitor tasks: https://code.earthengine.google.com/tasks")

    # Save log
    log_path = os.path.join(
        OUTPUT_BASE_FOLDER,
        f"export_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )
    try:
        with open(log_path, 'w') as f:
            f.write(f"GEE Export Log — {datetime.now()}\n")
            f.write(f"Export target : {EXPORT_TARGET}\n")
            f.write(f"Bands         : {SELECTED_BANDS}\n")
            f.write(f"Date range    : {START_DATE} → {END_DATE}\n")
            f.write(f"Buffer        : {BUFFER_DISTANCE}m\n")
            f.write(f"Resolution    : {RESOLUTION}m\n")
            f.write(f"Max cloud     : {MAX_CLOUD_COVER}%\n\n")
            f.write(f"Success  ({len(results['success'])}): {results['success']}\n")
            f.write(f"Failed   ({len(results['failed'])}): {results['failed']}\n")
            f.write(f"Skipped  ({len(results['skipped'])}): {results['skipped']}\n")
            f.write(f"No images({len(results['no_images'])}): {results['no_images']}\n")
        print(f"  Log saved: {log_path}")
    except Exception as e:
        print(f"  ⚠ Could not save log: {e}")


if __name__ == "__main__":
    main()