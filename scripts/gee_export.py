"""
gee_export.py — Google Earth Engine Batch Export
=================================================
PURPOSE:
  Submit batch export tasks to Google Earth Engine (GEE) that produce:
    • Sentinel-2 multispectral imagery (median composite, cloud-filtered)
    • SRTM Digital Elevation Model (DEM) elevation data
  for each selected Indian river.

  GEE exports are ASYNCHRONOUS — this script submits the tasks and returns.
  The actual export runs on Google's servers (takes 5–60 minutes per river).
  Monitor at: https://code.earthengine.google.com/tasks

WHY GEE for imagery?
  Google Earth Engine hosts the entire Sentinel-2 archive and SRTM DEM globally.
  We don't need to download raw satellite data — GEE creates a cloud-free median
  composite on-the-fly and exports only the final result. This saves terabytes of
  intermediate downloads.

CONFIG INJECTION:
  This script is NOT meant to be run directly (the defaults at the top are placeholders).
  run_step1.py reads config.json and injects overrides (SHAPEFILE_PATH, SPECIFIC_RIVERS,
  SELECTED_BANDS, etc.) into the source before exec()ing it.

DATA FLOW:
  Shapefile (.shp)
      ↓ geopandas.read_file()  →  river geometries (LineStrings/Polygons)
      ↓ create_buffered_geometries()  →  10 km buffers around each river
      ↓ gdf_row_to_ee_geometry()  →  Earth Engine Geometry objects
      ↓ export_sentinel_to_drive/gcs() + export_dem_to_drive/gcs()
      ↓ GEE API  →  export tasks running on Google's servers
      ↓ (asynchronous — Step 2 downloads results when tasks complete)

SENTINEL-2 PRODUCT:
  Collection: COPERNICUS/S2_SR_HARMONIZED
  - SR = Surface Reflectance (atmospherically corrected)
  - HARMONIZED = consistent processing across satellite generations
  - Median composite: most cloud-free pixel-wise median over the date range
  - Bands: configurable (B2=Blue, B3=Green, B4=Red, B5-B8A=Red Edge+NIR, B11/B12=SWIR)

DEM PRODUCT:
  Dataset: USGS/SRTMGL1_003
  - SRTM = Shuttle Radar Topography Mission (90m resolution, globally available)
  - Band: 'elevation' in metres above sea level
  - Used for flood modelling, slope analysis, catchment delineation

EXPORT FORMATS:
  All exports are GeoTIFF (.tif) with:
    crs='EPSG:4326'  (WGS84 geographic coordinates)
    scale=10         (10 metres per pixel for Sentinel-2)
    maxPixels=1e13   (allow very large exports — rivers can be long!)
"""

import ee             # Google Earth Engine Python API
import geopandas as gpd  # Read shapefiles, handle geometric operations
import json           # Convert GeoDataFrame geometry to GeoJSON for GEE
import os             # Path checks, create directories
import time           # time.time() for elapsed time, time.sleep() in polling loops
from datetime import datetime  # For timestamping the export log file

# =============================================================================
# CONFIGURATION — overridden at runtime by run_step1.py
# =============================================================================
# These are DEFAULT/PLACEHOLDER values.
# run_step1.py replaces them by injecting assignment statements into this
# source code BEFORE exec()ing it. Do NOT remove or rename these variables;
# run_step1.py maps config.json keys to exactly these names.

SHAPEFILE_PATH       = ""            # Absolute path to River_India_Final.shp
SPECIFIC_RIVERS      = []            # List of river names to process, e.g. ["Ganga", "Yamuna"]
OUTPUT_BASE_FOLDER   = ""            # Local root output folder
SENTINEL_SUBFOLDER   = "Sentinel"    # Sub-folder for Sentinel tiles inside OUTPUT_BASE_FOLDER
DEM_SUBFOLDER        = "DEM"         # Sub-folder for DEM tiles inside OUTPUT_BASE_FOLDER
BUFFER_DISTANCE      = 10000         # River buffer in metres (10 km each side of centreline)
RESOLUTION           = 10            # Export pixel size in metres
START_DATE           = "2025-01-01"  # Start of imagery date range
END_DATE             = "2025-12-31"  # End of imagery date range
MAX_CLOUD_COVER      = 10            # Maximum CLOUDY_PIXEL_PERCENTAGE per Sentinel-2 scene
DRIVE_FOLDER         = "River_Imagery_Batch"  # Google Drive root export folder
GEE_PROJECT          = "plucky-sight-423703-k5"  # GEE Cloud Project ID
MAX_CONCURRENT_TASKS = 100           # Max GEE tasks running simultaneously
SKIP_EXISTING        = True          # If True, skip rivers that already have local .tif files
SAVE_GEOMETRIES_ONLY = False         # Debug mode: only save GeoJSON geometries, no GEE export
PROCESS_BATCH_NUMBER = None          # Batch mode: process only the Nth batch of rivers (unused)

# Band selection — which Sentinel-2 bands to export.
# Overridden from config at runtime. Using all 10 analytical bands:
#   B2=Blue(490nm), B3=Green(560nm), B4=Red(665nm)
#   B5=Red Edge1(705nm), B6=Red Edge2(740nm), B7=Red Edge3(783nm)
#   B8=NIR(842nm), B8A=Narrow NIR(865nm)
#   B11=SWIR1(1610nm), B12=SWIR2(2190nm)
SELECTED_BANDS = ['B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B8A', 'B11', 'B12']

# Export target — where to send the exported files.
# "drive" = Google Drive (for Step 2 download to local disk)
# "gcs"   = Google Cloud Storage (for cloud VM merge in Step 3 alternative)
# "both"  = both destinations simultaneously
EXPORT_TARGET = "drive"

# GCS bucket name (hardcoded because it's an infrastructure setting)
GCS_BUCKET = "aiq-river-imagery"


# =============================================================================
# FUNCTIONS — GEOMETRY
# =============================================================================

def create_buffered_geometries(gdf, buffer_distance_m):
    """Create a buffered polygon around each river centreline.

    WHY buffer?
      GEE exports a rectangular bounding box. The buffer makes the bounding box
      wide enough to capture the floodplain on both sides of the river channel.
      10,000 m (10 km) each side captures most Indian river floodplains.

    HOW it works:
      1. Project to EPSG:3857 (Web Mercator, units in metres) — so buffer distance
         is in metres, not degrees. Buffering in degrees would give different widths
         at different latitudes (a degree of longitude narrows towards the poles).
      2. Create circular buffers around each geometry.
      3. Calculate area for the log summary.
      4. Reproject back to EPSG:4326 (WGS84 lat/lon) — required by GEE.

    Args:
        gdf:              GeoDataFrame with river geometries (any CRS)
        buffer_distance_m: Buffer radius in metres (e.g. 10000 for 10 km)

    Returns:
        GeoDataFrame in EPSG:4326 with buffered geometries and 'area_km2' column
    """
    print(f"\nCreating {buffer_distance_m}m buffers for {len(gdf)} rivers...")
    start = time.time()

    gdf_proj     = gdf.to_crs("EPSG:3857")             # Project to metres
    gdf_buffered = gdf_proj.copy()
    gdf_buffered['geometry'] = gdf_proj.geometry.buffer(buffer_distance_m)  # Actual buffer
    gdf_buffered['area_km2'] = gdf_buffered.geometry.area / 1e6  # Convert m² → km²
    gdf_buffered = gdf_buffered.to_crs("EPSG:4326")    # Reproject back to lat/lon for GEE

    print(f"  Done in {time.time()-start:.1f}s — total area: {gdf_buffered['area_km2'].sum():,.0f} km²")
    return gdf_buffered


def gdf_row_to_ee_geometry(row):
    """Convert a GeoDataFrame row's geometry to a GEE Geometry object.

    GEE requires its own Geometry type (ee.Geometry). The easiest bridge
    is via GeoJSON: geopandas can export geometry as GeoJSON, and GEE can
    load GeoJSON natively.

    Steps:
      1. Create a GeoSeries with just the one geometry (required by to_json())
      2. to_json() serialises it as a GeoJSON FeatureCollection string
      3. json.loads() parses it back to a Python dict
      4. Extract the first Feature's geometry object
      5. ee.Geometry() wraps it as a GEE Geometry

    Args:
        row: A pandas Series (one row from the GeoDataFrame)

    Returns:
        ee.Geometry — the river's buffered bounding polygon as a GEE object
    """
    geojson = json.loads(gpd.GeoSeries([row.geometry]).to_json())
    return ee.Geometry(geojson['features'][0]['geometry'])


def sanitize_filename(name):
    """Remove characters that are illegal in file/folder names on Windows and Unix.

    River names like "Burhi Gandak" → "Burhi Gandak" (spaces are fine)
    But names with / or : would break path construction.

    Illegal on Windows: < > : " / \\ | ? *
    We also strip leading/trailing spaces and dots (Windows quirks).

    Args:
        name: River name string

    Returns:
        Safe filename string (same name with illegal chars replaced by '_')
    """
    for ch in '<>:"/\\|?*':
        name = name.replace(ch, '_')
    return name.strip(' .')


# =============================================================================
# FUNCTIONS — GEE AUTH + TASK MANAGEMENT
# =============================================================================

def authenticate_gee():
    """Initialise the Earth Engine API with the configured GEE project.

    WHY try/except?
      If Application Default Credentials (ADC) are set up (via `gcloud auth
      application-default login`), ee.Initialize() works without prompting.
      If not, it falls back to ee.Authenticate() which opens a browser OAuth flow.

    The GEE_PROJECT parameter tells EE which Cloud Project to bill for tasks.
    EE tasks are free but the Cloud Storage bucket usage is billed to this project.
    """
    print("\nAuthenticating with Google Earth Engine...")
    try:
        ee.Initialize(project=GEE_PROJECT)
        print("  ✓ Authenticated")
    except Exception as e:
        print(f"  Default auth failed: {e} — attempting interactive...")
        ee.Authenticate()          # Opens browser for OAuth login
        ee.Initialize(project=GEE_PROJECT)
        print("  ✓ Authenticated (interactive)")


def get_running_task_count():
    """Count how many GEE tasks are currently RUNNING or READY (queued).

    GEE allows max 3000 tasks queued, but submitting too many at once can
    cause the account to hit limits. We throttle to MAX_CONCURRENT_TASKS.

    ee.batch.Task.list() returns all tasks (recent ones, paginated).
    We filter to only RUNNING + READY states (submitted but not finished).

    Returns:
        int — number of currently active GEE tasks
    """
    tasks = ee.batch.Task.list()
    return sum(1 for t in tasks if t.status().get('state') in ('RUNNING', 'READY'))


def wait_for_task_slots(max_concurrent):
    """Block until there are fewer than max_concurrent tasks running in GEE.

    Called before submitting each new export task. If GEE already has
    max_concurrent tasks running, we wait and poll every 10 seconds.

    The \\r (carriage return) keeps the counter on the same terminal line,
    giving a live "waiting..." display without flooding the log.

    Args:
        max_concurrent: Maximum number of simultaneous GEE tasks allowed
    """
    while True:
        running = get_running_task_count()
        if running < max_concurrent:
            return   # Slot available → proceed with export submission
        print(f"    ⏳ {running} tasks running, waiting for slots...", end='\r')
        time.sleep(10)   # Wait 10 seconds before polling again


def check_existing_files(river_name):
    """Check if local output files already exist for this river.

    Used when SKIP_EXISTING=True to avoid re-submitting GEE tasks for
    rivers whose output is already on disk.

    Checks two files:
      • <OUTPUT_BASE_FOLDER>/Sentinel/<safe_name>_sentinel.tif
      • <OUTPUT_BASE_FOLDER>/DEM/<safe_name>_dem.tif

    Args:
        river_name: River name (may contain spaces, used as-is with sanitization)

    Returns:
        Tuple[bool, bool] — (sentinel_exists, dem_exists)
    """
    safe = sanitize_filename(river_name)
    s = os.path.join(OUTPUT_BASE_FOLDER, SENTINEL_SUBFOLDER, f"{safe}_sentinel.tif")
    d = os.path.join(OUTPUT_BASE_FOLDER, DEM_SUBFOLDER,      f"{safe}_dem.tif")
    return os.path.exists(s), os.path.exists(d)


# =============================================================================
# FUNCTIONS — SENTINEL-2 EXPORT
# =============================================================================

def _build_sentinel_image(ee_geometry):
    """Build the Sentinel-2 composite image for a river's geometry.

    This is the core of the Sentinel export:
    1. Filter the S2_SR_HARMONIZED collection to the river's bounding box
    2. Keep only scenes within the configured date range
    3. Filter out scenes with too much cloud cover
    4. Take the per-pixel MEDIAN across all remaining scenes
       (median cleverly removes clouds: a cloud pixel is bright → median picks
       a non-cloudy pixel from the same location on a different day)
    5. Select only the configured bands (reduces file size)
    6. Clip to the river's exact geometry (removes excess pixels outside buffer)

    Why MEDIAN composite?
      Using .median() creates a single "cloud-free" image. Alternatives:
        .mosaic() = most recent pixel (keeps clouds if most recent is cloudy)
        .mean()   = averaged, but clouds pull values up
      Median is the standard practice for Sentinel-2 compositing.

    Args:
        ee_geometry: ee.Geometry — the buffered river polygon

    Returns:
        Tuple[ee.Image or None, int] — (composite_image, scene_count)
        Returns (None, 0) if no scenes match the filters.
    """
    collection = (
        ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
        .filterBounds(ee_geometry)        # Keep only scenes covering this area
        .filterDate(START_DATE, END_DATE) # Keep scenes in our date window
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', MAX_CLOUD_COVER))  # Cloud filter
    )
    count = collection.size().getInfo()   # Actual API call to count matching scenes
    if count == 0:
        return None, 0   # No scenes found → skip this river
    image = collection.median().select(SELECTED_BANDS).clip(ee_geometry)
    return image, count


def export_sentinel_to_drive(ee_geometry, river_name):
    """Submit a GEE task to export the Sentinel-2 composite to Google Drive.

    The exported file is saved to:
      Google Drive / <DRIVE_FOLDER>/Sentinel/<safe_name>_sentinel.tif
    (or split into tiles like ...-000.tif, ...-001.tif for large rivers)

    GEE parameters explained:
      description:    Task name visible in the GEE task monitor UI
      folder:         Google Drive folder path (relative to Drive root)
      fileNamePrefix: File name without extension (GEE appends .tif or -000.tif, -001.tif...)
      region:         The bounding geometry for the export
      scale:          Pixel resolution in metres (10m = native Sentinel-2 resolution)
      crs:            Coordinate reference system for the output
      maxPixels:      Limit to prevent accidental enormous exports; 1e13 = effectively unlimited
      fileFormat:     GeoTIFF (required for GIS analysis)

    Args:
        ee_geometry:  ee.Geometry — the buffered river polygon
        river_name:   str — river name (used to build the output filename)

    Returns:
        Tuple[ee.batch.Task or None, str, str]
          (task, filename, info_message)
          task is None if no scenes were found (nothing to export)
    """
    safe     = sanitize_filename(river_name)   # e.g. "Ganga", "Burhi_Gandak"
    filename = f"{safe}_sentinel"              # e.g. "Ganga_sentinel"

    image, count = _build_sentinel_image(ee_geometry)
    if image is None:
        return None, filename, "No images found"

    task = ee.batch.Export.image.toDrive(
        image=image,
        description=filename,
        folder=f"{DRIVE_FOLDER}/Sentinel",   # Drive path: River_Imagery_Batch/Sentinel
        fileNamePrefix=filename,
        region=ee_geometry,
        scale=RESOLUTION,
        crs='EPSG:4326',
        maxPixels=1e13,
        fileFormat='GeoTIFF',
    )
    task.start()   # Submit the task to GEE's queue (returns immediately)
    return task, filename, f"{count} images"


def export_sentinel_to_gcs(ee_geometry, river_name):
    """Submit a GEE task to export the Sentinel-2 composite to Google Cloud Storage.

    Almost identical to export_sentinel_to_drive() but uses toCloudStorage()
    and targets the GCS bucket instead of Drive.

    GCS path: gs://<GCS_BUCKET>/Sentinel/<safe_name>_sentinel.tif
    (or tiled: gs://<GCS_BUCKET>/Sentinel/<safe_name>_sentinel-000.tif, etc.)

    This is used when export_target = "gcs" or "both". The GCS path is where
    the cloud VM merge job (vm_merge_gcs.py) reads from.
    """
    safe     = sanitize_filename(river_name)
    filename = f"{safe}_sentinel"

    image, count = _build_sentinel_image(ee_geometry)
    if image is None:
        return None, filename, "No images found"

    task = ee.batch.Export.image.toCloudStorage(
        image=image,
        description=f"{filename}_gcs",           # Unique description for GCS tasks
        bucket=GCS_BUCKET,
        fileNamePrefix=f"Sentinel/{filename}",   # Path inside bucket
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
    """Load the SRTM DEM and clip it to the river geometry.

    USGS/SRTMGL1_003 is the 1 arc-second (~30m) SRTM dataset.
    We select only the 'elevation' band (ignoring other bands in the dataset).
    SRTM is a single static image (not a time-series), so no date filtering needed.

    Args:
        ee_geometry: ee.Geometry — the buffered river polygon

    Returns:
        ee.Image — the elevation band clipped to the river geometry
    """
    return (
        ee.Image('USGS/SRTMGL1_003')
        .select('elevation')      # Select only the elevation band (metres above sea level)
        .clip(ee_geometry)         # Clip to river buffer polygon
    )


def export_dem_to_drive(ee_geometry, river_name):
    """Submit a GEE task to export the SRTM DEM to Google Drive.

    Output: Google Drive / <DRIVE_FOLDER>/DEM/<safe_name>_dem.tif

    Note: Unlike Sentinel (which can have many tiles due to date filtering),
    DEM export is deterministic — always the same SRTM image, just cropped.
    The function returns (task, filename) without an 'info' string because
    there's no 'scene count' concept for a static dataset.

    Returns:
        Tuple[ee.batch.Task, str] — (task, filename)
    """
    safe     = sanitize_filename(river_name)
    filename = f"{safe}_dem"

    task = ee.batch.Export.image.toDrive(
        image=_build_dem_image(ee_geometry),
        description=filename,
        folder=f"{DRIVE_FOLDER}/DEM",   # Drive path: River_Imagery_Batch/DEM
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
    """Submit a GEE task to export the SRTM DEM to Google Cloud Storage.

    GCS path: gs://<GCS_BUCKET>/DEM/<safe_name>_dem.tif

    Parallel to export_dem_to_drive() but targets GCS.
    Used when export_target = "gcs" or "both".
    """
    safe     = sanitize_filename(river_name)
    filename = f"{safe}_dem"

    task = ee.batch.Export.image.toCloudStorage(
        image=_build_dem_image(ee_geometry),
        description=f"{filename}_gcs",
        bucket=GCS_BUCKET,
        fileNamePrefix=f"DEM/{filename}",   # Path inside bucket: DEM/<name>_dem.tif
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
    """Orchestrate the full GEE batch export for all selected rivers.

    This function:
      1. Loads and validates the shapefile
      2. Filters to SPECIFIC_RIVERS
      3. Creates 10km buffers
      4. Authenticates with GEE
      5. For each river: waits for task slots, then submits Sentinel + DEM exports
      6. Reports a summary and saves a log file

    The results dict tracks outcomes per river:
      success    — export tasks submitted successfully
      failed     — exception during submission
      skipped    — SKIP_EXISTING=True and local files already exist
      no_images  — no Sentinel-2 scenes found for the date range + cloud filter
      sentinel_tasks — list of submitted Sentinel task filenames
      dem_tasks      — list of submitted DEM task filenames
    """
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

    # ── Load and validate shapefile ────────────────────────────────────────────
    if not os.path.exists(SHAPEFILE_PATH):
        raise FileNotFoundError(f"Shapefile not found: {SHAPEFILE_PATH}")

    all_rivers_gdf = gpd.read_file(SHAPEFILE_PATH)
    print(f"\n  Loaded {len(all_rivers_gdf)} rivers from shapefile")
    print(f"  Columns: {list(all_rivers_gdf.columns)}")

    # ── Detect river name column ───────────────────────────────────────────────
    # Different shapefiles use different column names. We try a priority list.
    name_col = None
    for col in ("name", "NAME", "rivname", "RIVNAME", "RiverName", "River_Name", "river_name"):
        if col in all_rivers_gdf.columns:
            name_col = col
            break
    if name_col is None:
        raise ValueError(f"No river name column found. Available: {list(all_rivers_gdf.columns)}")

    # Standardise to 'name' for consistent downstream access
    if name_col != "name":
        all_rivers_gdf = all_rivers_gdf.rename(columns={name_col: "name"})

    # Keep only the name and geometry columns (discard other attributes)
    all_rivers_gdf = all_rivers_gdf[["name", "geometry"]]

    # ── Filter to selected rivers ─────────────────────────────────────────────
    if SPECIFIC_RIVERS:
        all_rivers_gdf = all_rivers_gdf[all_rivers_gdf['name'].isin(SPECIFIC_RIVERS)]
        print(f"  Filtered to {len(all_rivers_gdf)} selected rivers")
        # Warn about any requested rivers not found in the shapefile
        missing = set(SPECIFIC_RIVERS) - set(all_rivers_gdf['name'])
        if missing:
            print(f"  ⚠ Not found in shapefile: {missing}")

    if all_rivers_gdf.empty:
        print("ERROR: No rivers to process after filtering.")
        return

    # ── Create output directories on local disk ────────────────────────────────
    # These are needed for the SKIP_EXISTING check (check_existing_files()).
    # The actual export files land in Google Drive/GCS — local dirs hold downloaded copies.
    os.makedirs(os.path.join(OUTPUT_BASE_FOLDER, SENTINEL_SUBFOLDER), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_BASE_FOLDER, DEM_SUBFOLDER),      exist_ok=True)

    # Buffer all river geometries
    buffered_gdf = create_buffered_geometries(all_rivers_gdf, BUFFER_DISTANCE)

    # Authenticate to GEE (uses ADC or interactive browser login)
    authenticate_gee()

    # Track outcomes per river
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

        # ── Skip if local files already exist ─────────────────────────────────
        if SKIP_EXISTING:
            s_exists, d_exists = check_existing_files(river_name)
            if s_exists and d_exists:
                print(f"  ⏭  Skipped (local files already exist)")
                results['skipped'].append(river_name)
                continue

        try:
            # Wait until GEE has capacity for new tasks
            wait_for_task_slots(MAX_CONCURRENT_TASKS)

            # Convert the row's buffered geometry to a GEE Geometry object
            ee_geom = gdf_row_to_ee_geometry(row)

            # ── Submit Sentinel export ─────────────────────────────────────────
            sentinel_ok = False   # Track if at least one Sentinel task was submitted

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
                results['no_images'].append(river_name)   # No scenes found for this river

            # ── Submit DEM export ───────────────────────────────────────────────
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

    # ── Print summary ──────────────────────────────────────────────────────────
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

    # ── Save an export log to disk ─────────────────────────────────────────────
    # Persists a human-readable summary alongside the downloaded imagery files.
    # Useful for auditing which runs exported which rivers.
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
    # This block runs when executing the script directly (not via run_step1.py).
    # Without run_step1.py's config injection, it uses the placeholder defaults
    # at the top of this file — useful for quick manual testing.
    main()