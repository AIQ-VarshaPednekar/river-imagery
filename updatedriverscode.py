"""
Optimized Batch River Imagery Download Script
=============================================
Downloads satellite imagery (Sentinel-2 + DEM) for ALL rivers in a single efficient batch.

OPTIMIZATION: 
- First saves all river geometries to a local GeoPackage file (one-time DB call)
- Then uses the local file for all subsequent processing (no repeated DB calls)
- Supports batch export tasks to GEE in parallel
- AUTOMATICALLY downloads completed files from Google Drive to local machine

Workflow:
1. Run once with SAVE_GEOMETRIES_ONLY = True to save all river geometries locally
2. Run with SAVE_GEOMETRIES_ONLY = False to process imagery from local file
3. Script waits for GEE tasks to complete, then downloads to local folders

Organizes output into:
- Output/Sentinel/{river_name}_sentinel.tif
- Output/DEM/{river_name}_dem.tif
"""

import ee
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from shapely import wkb
import json
import os
import io
import time
from datetime import datetime
from urllib.parse import quote_plus

# Google Drive API imports
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pickle

# =============================================================================
# USER CONFIGURATION - MODIFY THESE VALUES
# =============================================================================

# STEP 1: Set to True to download all geometries from DB and save locally (run once)
# STEP 2: Set to False to process imagery using the saved local file
SAVE_GEOMETRIES_ONLY = False

# Batch size for saving geometries (number of rivers per file)
BATCH_SIZE = 10

# Folder to store river geometry batch files
GEOMETRIES_FOLDER = r"D:\River Cleaning\Kruti\river_geometries_batches"

# Process specific batch number (None = process all batches, or specify batch number like 1, 2, 3...)
PROCESS_BATCH_NUMBER = None
SHAPEFILE_PATH = r"C:\Users\My Pc\Downloads\River_India_Final 2\River_India_Final.shp"

# Buffer distance in meters (applied to all rivers)
BUFFER_DISTANCE = 10000  # 10 km buffer

# Output resolution in meters
RESOLUTION = 10  # 10 meters

# Date range for satellite imagery
START_DATE = "2024-01-01"
END_DATE = "2024-12-31"

# Maximum cloud cover percentage
MAX_CLOUD_COVER = 10

# Google Drive folder name (will be created if doesn't exist)
DRIVE_FOLDER = "River_Imagery_Batch"

# Output folder structure
OUTPUT_BASE_FOLDER = r"C:\Users\My Pc\Documents\river project aiq\Imagery_Output"
SENTINEL_SUBFOLDER = "Sentinel"
DEM_SUBFOLDER = "DEM"

# Maximum number of concurrent GEE export tasks (GEE limit is typically 3000)
MAX_CONCURRENT_TASKS = 100

# Skip rivers that already have imagery downloaded
SKIP_EXISTING = True

# Process specific rivers only (leave empty list [] to process all)
# Example: SPECIFIC_RIVERS = ["Ambika", "Ganga", "Yamuna"]
SPECIFIC_RIVERS = ["Amba"]

# =============================================================================
# GOOGLE DRIVE DOWNLOAD CONFIGURATION
# =============================================================================

# Enable automatic download from Google Drive after GEE tasks complete
AUTO_DOWNLOAD_FROM_DRIVE = True

# Path to Google OAuth credentials file (client_secret_*.json)
# Token file to store user's access and refresh tokens
GOOGLE_CREDENTIALS_FILE = r"C:\Users\My Pc\Documents\river project aiq\client_secret.json"
TOKEN_FILE = r"C:\Users\My Pc\Documents\river project aiq\drive_token.pickle"


# Local download paths (separate for Sentinel and DEM)
LOCAL_DOWNLOAD_PATH_SENTINEL = r"C:\Users\My Pc\Documents\river project aiq\Imagery_Output\Sentinel"
LOCAL_DOWNLOAD_PATH_DEM = r"C:\Users\My Pc\Documents\river project aiq\Imagery_Output\DEM"

# Wait for all GEE tasks to complete before downloading (True), or start downloading as tasks complete (False)
WAIT_ALL_TASKS_COMPLETE = False

# Task polling interval in seconds (how often to check task status)
TASK_POLL_INTERVAL = 30

# Maximum wait time for tasks in hours (0 = unlimited)
MAX_WAIT_HOURS = 12

# Delete files from Google Drive after successful download
DELETE_AFTER_DOWNLOAD = False

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================

POSTGRES_CONFIG = {
    'host': 'aiqspace-postgresql.postgres.database.azure.com',
    'port': 5432,
    'database': 'india_gis_db',
    'user': 'techaiqspace',
    'password': 'StRpost!g4resql0rd#2025gis'
}

# =============================================================================
# GEE CONFIGURATION
# =============================================================================

GEE_PROJECT = "plucky-sight-423703-k5"

# =============================================================================
# FUNCTIONS - GEOMETRY HANDLING
# =============================================================================

def get_db_connection():
    """Create database connection."""
    password = quote_plus(POSTGRES_CONFIG['password'])
    conn_string = f"postgresql://{POSTGRES_CONFIG['user']}:{password}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}?sslmode=require"
    return create_engine(conn_string)


def download_all_geometries_from_db():
    """
    Download ALL river geometries from database in a single query.
    Returns a GeoDataFrame with all rivers.
    """
    print(f"\n{'='*70}")
    print("DOWNLOADING ALL RIVER GEOMETRIES FROM DATABASE")
    print('='*70)
    
    engine = get_db_connection()
    
    # Single query to get all rivers with their geometries
    query = text("""
        SELECT 
            name,
            ST_AsBinary(ST_Union(geom)) as geom
        FROM vector.river_cleaning
        WHERE name IS NOT NULL
        GROUP BY name
        ORDER BY name
    """)
    
    print("Executing query (this may take a moment)...")
    start_time = time.time()
    
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
    
    elapsed = time.time() - start_time
    print(f"✓ Query completed in {elapsed:.1f} seconds")
    print(f"✓ Found {len(rows)} rivers")
    
    # Create GeoDataFrame
    geometries = []
    names = []
    
    for row in rows:
        try:
            geom = wkb.loads(bytes(row.geom))
            geometries.append(geom)
            names.append(row.name)
        except Exception as e:
            print(f"  ⚠ Error parsing geometry for '{row.name}': {e}")
    
    gdf = gpd.GeoDataFrame({'name': names}, geometry=geometries, crs="EPSG:4326")
    
    print(f"✓ Created GeoDataFrame with {len(gdf)} rivers")
    
    return gdf


def save_geometries_in_batches(gdf, folder, batch_size):
    """
    Save GeoDataFrame to multiple batch files.
    Each file contains batch_size rivers.
    """
    print(f"\nSaving geometries in batches of {batch_size} to: {folder}")
    
    # Create directory if needed
    os.makedirs(folder, exist_ok=True)
    
    total_rivers = len(gdf)
    num_batches = (total_rivers + batch_size - 1) // batch_size  # Ceiling division
    
    batch_files = []
    
    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, total_rivers)
        
        # Get batch subset
        batch_gdf = gdf.iloc[start_idx:end_idx].copy()
        
        # Create filename with zero-padded batch number
        batch_filename = f"rivers_batch_{batch_num + 1:03d}.gpkg"
        batch_filepath = os.path.join(folder, batch_filename)
        
        # Save batch
        batch_gdf.to_file(batch_filepath, driver='GPKG')
        
        file_size = os.path.getsize(batch_filepath) / (1024 * 1024)  # MB
        river_names = batch_gdf['name'].tolist()
        
        print(f"  Batch {batch_num + 1}/{num_batches}: {len(batch_gdf)} rivers -> {batch_filename} ({file_size:.2f} MB)")
        print(f"    Rivers: {', '.join(river_names[:3])}{'...' if len(river_names) > 3 else ''}")
        
        batch_files.append(batch_filepath)
    
    print(f"\n✓ Created {num_batches} batch files")
    
    # Save batch index file
    index_path = os.path.join(folder, "_batch_index.txt")
    with open(index_path, 'w') as f:
        f.write(f"Batch Index - Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total rivers: {total_rivers}\n")
        f.write(f"Batch size: {batch_size}\n")
        f.write(f"Number of batches: {num_batches}\n\n")
        
        for batch_num in range(num_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_rivers)
            batch_gdf = gdf.iloc[start_idx:end_idx]
            f.write(f"\nBatch {batch_num + 1}:\n")
            for name in batch_gdf['name'].tolist():
                f.write(f"  - {name}\n")
    
    print(f"✓ Batch index saved: {index_path}")
    
    return batch_files


def load_geometries_from_batch(folder, batch_number=None):
    """
    Load river geometries from batch files.
    If batch_number is specified, load only that batch.
    Otherwise, load all batches and combine.
    """
    print(f"\n{'='*70}")
    print("LOADING RIVER GEOMETRIES FROM BATCH FILES")
    print('='*70)
    print(f"Folder: {folder}")
    
    if not os.path.exists(folder):
        raise FileNotFoundError(f"Geometries folder not found: {folder}\n"
                                f"Run with SAVE_GEOMETRIES_ONLY = True first!")
    
    # Find all batch files
    batch_files = sorted([f for f in os.listdir(folder) if f.startswith('rivers_batch_') and f.endswith('.gpkg')])
    
    if not batch_files:
        raise FileNotFoundError(f"No batch files found in: {folder}")
    
    print(f"Found {len(batch_files)} batch files")
    
    if batch_number is not None:
        # Load specific batch
        batch_filename = f"rivers_batch_{batch_number:03d}.gpkg"
        if batch_filename not in batch_files:
            raise ValueError(f"Batch {batch_number} not found. Available: 1-{len(batch_files)}")
        
        filepath = os.path.join(folder, batch_filename)
        gdf = gpd.read_file(filepath)
        print(f"✓ Loaded batch {batch_number}: {len(gdf)} rivers")
    else:
        # Load all batches and combine
        all_gdfs = []
        for batch_file in batch_files:
            filepath = os.path.join(folder, batch_file)
            gdf = gpd.read_file(filepath)
            all_gdfs.append(gdf)
            print(f"  ✓ Loaded {batch_file}: {len(gdf)} rivers")
        
        gdf = gpd.GeoDataFrame(pd.concat(all_gdfs, ignore_index=True), crs="EPSG:4326")
        print(f"\n✓ Combined all batches: {len(gdf)} rivers total")
    
    return gdf


def load_geometries_from_file(filepath):
    """Load river geometries from local file (legacy single-file support)."""
    print(f"\n{'='*70}")
    print("LOADING RIVER GEOMETRIES FROM LOCAL FILE")
    print('='*70)
    print(f"File: {filepath}")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Geometries file not found: {filepath}\n"
                                f"Run with SAVE_GEOMETRIES_ONLY = True first!")
    
    gdf = gpd.read_file(filepath)
    print(f"✓ Loaded {len(gdf)} rivers from local file")
    
    return gdf


def create_buffered_geometries(gdf, buffer_distance_m):
    """
    Create buffers for ALL rivers at once (vectorized operation).
    Returns GeoDataFrame with buffered geometries.
    """
    print(f"\nCreating {buffer_distance_m}m buffers for all rivers...")
    start_time = time.time()
    
    # Project to Web Mercator for accurate meter-based buffer
    gdf_projected = gdf.to_crs("EPSG:3857")
    
    # Vectorized buffer operation
    gdf_buffered = gdf_projected.copy()
    gdf_buffered['geometry'] = gdf_projected.geometry.buffer(buffer_distance_m)
    
    # Calculate areas
    gdf_buffered['area_km2'] = gdf_buffered.geometry.area / 1e6
    
    # Convert back to WGS84 for GEE
    gdf_buffered = gdf_buffered.to_crs("EPSG:4326")
    
    elapsed = time.time() - start_time
    total_area = gdf_buffered['area_km2'].sum()
    
    print(f"✓ Buffers created in {elapsed:.1f} seconds")
    print(f"✓ Total buffered area: {total_area:,.0f} km²")
    
    return gdf_buffered


# =============================================================================
# FUNCTIONS - GEE OPERATIONS
# =============================================================================

def authenticate_gee():
    """Authenticate with Google Earth Engine."""
    print("\nAuthenticating with Google Earth Engine...")
    
    try:
        ee.Initialize(project=GEE_PROJECT)
        print("✓ GEE authenticated via default method")
    except Exception as e:
        print(f"Default auth failed: {e}")
        print("Attempting interactive authentication...")
        ee.Authenticate()
        ee.Initialize(project=GEE_PROJECT)
        print("✓ GEE authenticated interactively")


def gdf_row_to_ee_geometry(row):
    """Convert a single GeoDataFrame row to Earth Engine Geometry."""
    geojson = json.loads(gpd.GeoSeries([row.geometry]).to_json())
    ee_geom = ee.Geometry(geojson['features'][0]['geometry'])
    return ee_geom


def sanitize_filename(name):
    """Sanitize river name for use as filename."""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        name = name.replace(char, '_')
    name = name.strip(' .')
    return name


def get_running_task_count():
    """Get count of currently running GEE tasks."""
    tasks = ee.batch.Task.list()
    running = sum(1 for t in tasks if t.status().get('state') in ['RUNNING', 'READY'])
    return running


def wait_for_task_slots(max_concurrent):
    """Wait until there are available task slots."""
    while True:
        running = get_running_task_count()
        if running < max_concurrent:
            return
        print(f"    ⏳ {running} tasks running, waiting for slots...", end='\r')
        time.sleep(10)


def export_sentinel_to_drive(ee_geometry, river_name, start_date, end_date, max_cloud, 
                              resolution, drive_folder):
    """Export Sentinel-2 imagery with ALL bands to Google Drive."""
    
    safe_name = sanitize_filename(river_name)
    filename = f"{safe_name}_sentinel"
    
    # Load Sentinel-2 collection
    collection = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED') \
        .filterBounds(ee_geometry) \
        .filterDate(start_date, end_date) \
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', max_cloud))
    
    # Get collection size
    count = collection.size().getInfo()
    
    if count == 0:
        return None, filename, "No images found"
    
    # Create median composite
    composite = collection.median()
    
    # Select ALL spectral bands
    all_bands = ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7', 'B8', 'B8A', 'B9', 'B11', 'B12']
    image = composite.select(all_bands)
    
    # Clip to area of interest
    image = image.clip(ee_geometry)
    
    # Create export task
    task = ee.batch.Export.image.toDrive(
        image=image,
        description=filename,
        folder=f"{drive_folder}/Sentinel",
        fileNamePrefix=filename,
        region=ee_geometry,
        scale=resolution,
        crs='EPSG:4326',
        maxPixels=1e13,
        fileFormat='GeoTIFF'
    )
    
    task.start()
    return task, filename, f"{count} images"


def export_dem_to_drive(ee_geometry, river_name, resolution, drive_folder):
    """Export SRTM DEM to Google Drive."""
    
    safe_name = sanitize_filename(river_name)
    filename = f"{safe_name}_dem"
    
    # Load SRTM DEM
    dem = ee.Image('USGS/SRTMGL1_003')
    
    # Clip to area of interest
    dem = dem.clip(ee_geometry)
    
    # Create export task
    task = ee.batch.Export.image.toDrive(
        image=dem,
        description=filename,
        folder=f"{drive_folder}/DEM",
        fileNamePrefix=filename,
        region=ee_geometry,
        scale=resolution,
        crs='EPSG:4326',
        maxPixels=1e13,
        fileFormat='GeoTIFF'
    )
    
    task.start()
    return task, filename


def check_existing_files(river_name, output_folder, sentinel_subfolder, dem_subfolder):
    """Check if imagery already exists for a river."""
    safe_name = sanitize_filename(river_name)
    
    sentinel_path = os.path.join(output_folder, sentinel_subfolder, f"{safe_name}_sentinel.tif")
    dem_path = os.path.join(output_folder, dem_subfolder, f"{safe_name}_dem.tif")
    
    return os.path.exists(sentinel_path), os.path.exists(dem_path)


# =============================================================================
# GOOGLE DRIVE DOWNLOAD FUNCTIONS
# =============================================================================

# Google Drive API scope
SCOPES = ['https://www.googleapis.com/auth/drive.readonly', 
          'https://www.googleapis.com/auth/drive.file']


def authenticate_google_drive():
    """
    Authenticate with Google Drive API and return service object.
    Uses OAuth2 flow with local credentials file.
    """
    print("\nAuthenticating with Google Drive...")
    
    creds = None
    
    # Check if token file exists with saved credentials
    if os.path.exists(TOKEN_FILE):
        print("  Loading saved credentials...")
        with open(TOKEN_FILE, 'rb') as token:
            creds = pickle.load(token)
    
    # If no valid credentials, do the OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("  Refreshing expired credentials...")
            creds.refresh(Request())
        else:
            if not os.path.exists(GOOGLE_CREDENTIALS_FILE):
                raise FileNotFoundError(
                    f"Google credentials file not found: {GOOGLE_CREDENTIALS_FILE}\n"
                    f"Please download OAuth 2.0 credentials from Google Cloud Console."
                )
            print("  Starting OAuth2 flow (browser will open)...")
            flow = InstalledAppFlow.from_client_secrets_file(GOOGLE_CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save credentials for next run
        with open(TOKEN_FILE, 'wb') as token:
            pickle.dump(creds, token)
        print("  ✓ Credentials saved for future use")
    
    # Build Drive service
    service = build('drive', 'v3', credentials=creds)
    print("✓ Google Drive authenticated successfully")
    
    return service


def find_drive_folder(service, folder_name, parent_id=None):
    """
    Find a folder in Google Drive by name.
    Returns folder ID or None if not found.
    """
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    results = service.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name)'
    ).execute()
    
    files = results.get('files', [])
    return files[0]['id'] if files else None


def list_drive_files(service, folder_id):
    """
    List all files in a Google Drive folder.
    Returns list of file dictionaries with id, name, size.
    """
    files = []
    page_token = None
    
    while True:
        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            spaces='drive',
            fields='nextPageToken, files(id, name, mimeType, size)',
            pageToken=page_token,
            pageSize=100
        ).execute()
        
        files.extend(results.get('files', []))
        page_token = results.get('nextPageToken')
        
        if not page_token:
            break
    
    return files


def download_file_from_drive(service, file_id, file_name, local_path):
    """
    Download a file from Google Drive to local path.
    Shows progress during download.
    """
    # Create directory if needed
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    # Request file content
    request = service.files().get_media(fileId=file_id)
    
    # Download with progress
    fh = io.FileIO(local_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            progress = int(status.progress() * 100)
            print(f"      Downloading: {progress}%", end='\r')
    
    fh.close()
    print(f"      ✓ Downloaded: {file_name}                    ")
    
    return local_path


def delete_file_from_drive(service, file_id, file_name):
    """
    Delete a file from Google Drive.
    """
    try:
        service.files().delete(fileId=file_id).execute()
        print(f"      ✓ Deleted from Drive: {file_name}")
        return True
    except Exception as e:
        print(f"      ⚠ Failed to delete {file_name}: {e}")
        return False


def download_completed_files_from_drive(service, drive_folder_name, sentinel_path, dem_path, 
                                        expected_sentinel_files, expected_dem_files, 
                                        delete_after=False):
    """
    Download all completed files from Google Drive to local paths.
    Organizes by Sentinel and DEM subfolders.
    """
    print(f"\n{'='*70}")
    print("DOWNLOADING FILES FROM GOOGLE DRIVE")
    print('='*70)
    
    # Create local folders
    os.makedirs(sentinel_path, exist_ok=True)
    os.makedirs(dem_path, exist_ok=True)
    
    # Find main Drive folder
    results = {'sentinel': [], 'dem': [], 'errors': []}

    # Find folders directly (GEE creates them with slash in name)
    sentinel_folder_id = find_drive_folder(service, f"{drive_folder_name}/Sentinel")
    dem_folder_id = find_drive_folder(service, f"{drive_folder_name}/DEM")

    print(f"  ✓ Searching Drive folders...")

    if sentinel_folder_id:
        print(f"\n  📡 Downloading Sentinel files...")
        sentinel_files = list_drive_files(service, sentinel_folder_id)
        
        for file_info in sentinel_files:
            if file_info['name'].endswith('.tif'):
                try:
                    local_file = os.path.join(sentinel_path, file_info['name'])
                    
                    # Skip if already exists locally
                    if os.path.exists(local_file):
                        print(f"      ⏭ Already exists: {file_info['name']}")
                        results['sentinel'].append(file_info['name'])
                        continue
                    
                    download_file_from_drive(service, file_info['id'], file_info['name'], local_file)
                    results['sentinel'].append(file_info['name'])
                    
                    if delete_after:
                        delete_file_from_drive(service, file_info['id'], file_info['name'])
                        
                except Exception as e:
                    print(f"      ✗ Error downloading {file_info['name']}: {e}")
                    results['errors'].append((file_info['name'], str(e)))
    else:
        print(f"  ⚠ Sentinel subfolder not found in Drive")
    
    # Download DEM files
    if dem_folder_id:
        print(f"\n  🗻 Downloading DEM files...")
        dem_files = list_drive_files(service, dem_folder_id)
        
        for file_info in dem_files:
            if file_info['name'].endswith('.tif'):
                try:
                    local_file = os.path.join(dem_path, file_info['name'])
                    
                    # Skip if already exists locally
                    if os.path.exists(local_file):
                        print(f"      ⏭ Already exists: {file_info['name']}")
                        results['dem'].append(file_info['name'])
                        continue
                    
                    download_file_from_drive(service, file_info['id'], file_info['name'], local_file)
                    results['dem'].append(file_info['name'])
                    
                    if delete_after:
                        delete_file_from_drive(service, file_info['id'], file_info['name'])
                        
                except Exception as e:
                    print(f"      ✗ Error downloading {file_info['name']}: {e}")
                    results['errors'].append((file_info['name'], str(e)))
    else:
        print(f"  ⚠ DEM subfolder not found in Drive")
    
    # Summary
    print(f"\n  Download Summary:")
    print(f"    Sentinel: {len(results['sentinel'])} files -> {sentinel_path}")
    print(f"    DEM: {len(results['dem'])} files -> {dem_path}")
    if results['errors']:
        print(f"    Errors: {len(results['errors'])}")
    
    return results


def wait_and_download_gee_tasks(service, task_names, drive_folder_name, sentinel_path, dem_path,
                                 poll_interval=30, max_wait_hours=12, delete_after=False):
    """
    Wait for GEE tasks to complete and download files as they finish.
    
    Args:
        service: Google Drive service object
        task_names: List of expected task names (filenames without extension)
        drive_folder_name: Name of the Drive folder
        sentinel_path: Local path for Sentinel files
        dem_path: Local path for DEM files
        poll_interval: Seconds between status checks
        max_wait_hours: Maximum hours to wait (0 = unlimited)
        delete_after: Delete from Drive after successful download
    """
    print(f"\n{'='*70}")
    print("MONITORING GEE TASKS & DOWNLOADING COMPLETED FILES")
    print('='*70)
    
    start_time = time.time()
    max_wait_seconds = max_wait_hours * 3600 if max_wait_hours > 0 else float('inf')
    
    downloaded_files = set()
    failed_tasks = set()
    
    # Create local folders
    os.makedirs(sentinel_path, exist_ok=True)
    os.makedirs(dem_path, exist_ok=True)
    
    while True:
        # Check time limit
        elapsed = time.time() - start_time
        if elapsed > max_wait_seconds:
            print(f"\n  ⚠ Maximum wait time ({max_wait_hours}h) exceeded")
            break
        
        # Get GEE task statuses
        tasks = ee.batch.Task.list()
        task_status = {}
        
        for task in tasks:
            status = task.status()
            name = status.get('description', '')
            state = status.get('state', 'UNKNOWN')
            task_status[name] = state
        
        # Count status
        completed = sum(1 for name in task_names if task_status.get(name) == 'COMPLETED')
        running = sum(1 for name in task_names if task_status.get(name) in ['RUNNING', 'READY'])
        failed = sum(1 for name in task_names if task_status.get(name) == 'FAILED')
        
        total = len(task_names)
        downloaded = len(downloaded_files)
        
        print(f"\n  Status: {completed}/{total} completed, {running} running, {failed} failed, {downloaded} downloaded")
        print(f"  Elapsed: {elapsed/60:.1f} min")
        
        # Check for newly completed tasks and download
        for task in tasks:
            status = task.status()
            name = status.get('description', '')
            state = status.get('state', '')
            
            if name not in task_names:
                continue
            
            if state == 'COMPLETED' and name not in downloaded_files:
                # Determine type (Sentinel or DEM)
                is_sentinel = '_sentinel' in name.lower()
                is_dem = '_dem' in name.lower()
                
                # Wait a moment for file to appear in Drive
                time.sleep(5)
                
                # Find and download the file
                main_folder_id = find_drive_folder(service, drive_folder_name)
                if True:
                    if is_sentinel:
                        subfolder_id = find_drive_folder(service, f"{drive_folder_name}/Sentinel")
                        local_path = sentinel_path
                    elif is_dem:
                        subfolder_id = find_drive_folder(service, f"{drive_folder_name}/DEM")
                        local_path = dem_path
                    else:
                        continue
                    
                    if subfolder_id:
                        files = list_drive_files(service, subfolder_id)
                        for f in files:
                            if f['name'].startswith(name) and f['name'].endswith('.tif'):
                                local_file = os.path.join(local_path, f['name'])
                                if not os.path.exists(local_file):
                                    try:
                                        download_file_from_drive(service, f['id'], f['name'], local_file)
                                        downloaded_files.add(name)
                                        if delete_after:
                                            delete_file_from_drive(service, f['id'], f['name'])
                                    except Exception as e:
                                        print(f"      ✗ Download error: {e}")
                                else:
                                    downloaded_files.add(name)
                                break
            
            elif state == 'FAILED' and name not in failed_tasks:
                error = status.get('error_message', 'Unknown error')
                print(f"    ✗ Task failed: {name} - {error}")
                failed_tasks.add(name)
        
        # Check if all done
        if completed + failed >= total:
            print(f"\n  ✓ All tasks finished ({completed} completed, {failed} failed)")
            break
        
        # Wait before next check
        print(f"  Waiting {poll_interval}s before next check...", end='\r')
        time.sleep(poll_interval)
    
    # Final download sweep to catch any missed files
    print("\n  Final download sweep...")
    final_results = download_completed_files_from_drive(
        service, drive_folder_name, sentinel_path, dem_path,
        [], [], delete_after
    )
    
    return {
        'downloaded': len(downloaded_files),
        'failed_tasks': len(failed_tasks),
        'sentinel_files': final_results['sentinel'],
        'dem_files': final_results['dem']
    }


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    print("\n" + "="*70)
    print("   OPTIMIZED BATCH RIVER IMAGERY DOWNLOAD")
    print("="*70)
    
    # ==========================================================================
    # STEP 1: Save all geometries to local file (run once)
    # ==========================================================================
    if SAVE_GEOMETRIES_ONLY:
        print("\n*** MODE: SAVE GEOMETRIES IN BATCHES ***")
        print(f"This will download all river geometries from the database")
        print(f"and save them in batches of {BATCH_SIZE} rivers each.")
        
        # Download from database
        all_rivers_gdf = download_all_geometries_from_db()
        
        # Save in batches
        batch_files = save_geometries_in_batches(all_rivers_gdf, GEOMETRIES_FOLDER, BATCH_SIZE)
        
        print(f"\n{'='*70}")
        print("DONE! Geometries saved in batches successfully.")
        print("="*70)
        print(f"\nCreated {len(batch_files)} batch files in: {GEOMETRIES_FOLDER}")
        print(f"\nNext steps:")
        print(f"1. Set SAVE_GEOMETRIES_ONLY = False")
        print(f"2. Optionally set PROCESS_BATCH_NUMBER = 1 to process only batch 1")
        print(f"   Or leave PROCESS_BATCH_NUMBER = None to process all batches")
        print(f"3. Run this script again to download imagery")
        
        return
    
    # ==========================================================================
    # STEP 2: Process imagery using local file
    # ==========================================================================
    batch_info = f"Batch {PROCESS_BATCH_NUMBER}" if PROCESS_BATCH_NUMBER else "All batches"
    print(f"\n*** MODE: DOWNLOAD IMAGERY ({batch_info}) ***")
    print(f"\nConfiguration:")
    print(f"  Geometries folder: {GEOMETRIES_FOLDER}")
    print(f"  Processing: {batch_info}")
    print(f"  Buffer distance: {BUFFER_DISTANCE}m")
    print(f"  Resolution: {RESOLUTION}m")
    print(f"  Date range: {START_DATE} to {END_DATE}")
    print(f"  Max cloud cover: {MAX_CLOUD_COVER}%")
    print(f"  Drive folder: {DRIVE_FOLDER}")
    print(f"  Local output: {OUTPUT_BASE_FOLDER}")
    print(f"  Max concurrent tasks: {MAX_CONCURRENT_TASKS}")
    print(f"  Skip existing: {SKIP_EXISTING}")
    
    # Create output folders
    sentinel_folder = os.path.join(OUTPUT_BASE_FOLDER, SENTINEL_SUBFOLDER)
    dem_folder = os.path.join(OUTPUT_BASE_FOLDER, DEM_SUBFOLDER)
    os.makedirs(sentinel_folder, exist_ok=True)
    os.makedirs(dem_folder, exist_ok=True)
    
    # Load geometries from shapefile
    all_rivers_gdf = gpd.read_file(SHAPEFILE_PATH)

    # Rename column to 'name' if it's different
    # Check your attribute table - change "NAME" below to match your column
    all_rivers_gdf = all_rivers_gdf.rename(columns={"rivname": "name"})

    # Keep only name and geometry columns
    all_rivers_gdf = all_rivers_gdf[["name", "geometry"]]
    
    # Filter to specific rivers if specified
    if SPECIFIC_RIVERS:
        all_rivers_gdf = all_rivers_gdf[all_rivers_gdf['name'].isin(SPECIFIC_RIVERS)]
        print(f"\n  Filtered to {len(all_rivers_gdf)} specific rivers")
    
    # Create buffers for all rivers at once
    buffered_gdf = create_buffered_geometries(all_rivers_gdf, BUFFER_DISTANCE)
    
    # Authenticate with GEE
    authenticate_gee()
    
    # Track results
    results = {
        'success': [],
        'failed': [],
        'skipped': [],
        'no_images': [],
        'sentinel_tasks': [],
        'dem_tasks': []
    }
    
    # Process each river
    print(f"\n{'='*70}")
    print("STARTING BATCH EXPORT TO GOOGLE EARTH ENGINE")
    print('='*70)
    
    total = len(buffered_gdf)
    start_time = time.time()
    
    for idx, row in buffered_gdf.iterrows():
        river_name = row['name']
        i = list(buffered_gdf.index).index(idx) + 1
        
        print(f"\n[{i}/{total}] {river_name}")
        
        # Check if already exists
        if SKIP_EXISTING:
            sentinel_exists, dem_exists = check_existing_files(
                river_name, OUTPUT_BASE_FOLDER, SENTINEL_SUBFOLDER, DEM_SUBFOLDER
            )
            if sentinel_exists and dem_exists:
                print(f"    ⏭ Skipped (already exists)")
                results['skipped'].append(river_name)
                continue
        
        try:
            # Wait for task slots if needed
            wait_for_task_slots(MAX_CONCURRENT_TASKS)
            
            # Convert to EE geometry
            ee_geometry = gdf_row_to_ee_geometry(row)
            
            area_km2 = row.get('area_km2', 0)
            print(f"    Buffer area: {area_km2:.2f} km²")
            
            # Export Sentinel-2
            sentinel_task, sentinel_filename, sentinel_info = export_sentinel_to_drive(
                ee_geometry=ee_geometry,
                river_name=river_name,
                start_date=START_DATE,
                end_date=END_DATE,
                max_cloud=MAX_CLOUD_COVER,
                resolution=RESOLUTION,
                drive_folder=DRIVE_FOLDER
            )
            
            if sentinel_task:
                print(f"    ✓ Sentinel export started ({sentinel_info})")
                results['sentinel_tasks'].append(sentinel_filename)
            else:
                print(f"    ⚠ Sentinel: {sentinel_info}")
                results['no_images'].append(river_name)
            
            # Export DEM
            dem_task, dem_filename = export_dem_to_drive(
                ee_geometry=ee_geometry,
                river_name=river_name,
                resolution=RESOLUTION,
                drive_folder=DRIVE_FOLDER
            )
            
            print(f"    ✓ DEM export started")
            results['dem_tasks'].append(dem_filename)
            
            results['success'].append(river_name)
            
        except Exception as e:
            print(f"    ✗ Error: {e}")
            results['failed'].append((river_name, str(e)))
    
    elapsed = time.time() - start_time
    
    # Summary
    print(f"\n{'='*70}")
    print("BATCH EXPORT SUMMARY")
    print('='*70)
    print(f"\n  Total rivers: {total}")
    print(f"  ✓ Success: {len(results['success'])}")
    print(f"  ✗ Failed: {len(results['failed'])}")
    print(f"  ⏭ Skipped: {len(results['skipped'])}")
    print(f"  ⚠ No images: {len(results['no_images'])}")
    print(f"\n  📤 Sentinel tasks started: {len(results['sentinel_tasks'])}")
    print(f"  📤 DEM tasks started: {len(results['dem_tasks'])}")
    print(f"\n  ⏱ Total time: {elapsed/60:.1f} minutes")
    
    if results['failed']:
        print(f"\n  Failed rivers:")
        for r, e in results['failed']:
            print(f"    - {r}: {e}")
    
    print(f"\n  Check task status at:")
    print(f"  https://code.earthengine.google.com/tasks")
    
    # Save log file
    batch_suffix = f"_batch{PROCESS_BATCH_NUMBER:03d}" if PROCESS_BATCH_NUMBER else "_all"
    log_path = os.path.join(OUTPUT_BASE_FOLDER, f"batch_download_log{batch_suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
    with open(log_path, 'w') as f:
        f.write(f"Optimized Batch River Imagery Download Log\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total time: {elapsed/60:.1f} minutes\n")
        f.write(f"\nConfiguration:\n")
        f.write(f"  Geometries folder: {GEOMETRIES_FOLDER}\n")
        f.write(f"  Batch processed: {batch_info}\n")
        f.write(f"  Buffer: {BUFFER_DISTANCE}m\n")
        f.write(f"  Resolution: {RESOLUTION}m\n")
        f.write(f"  Date range: {START_DATE} to {END_DATE}\n")
        f.write(f"  Max cloud cover: {MAX_CLOUD_COVER}%\n")
        f.write(f"\nResults:\n")
        f.write(f"  Success: {len(results['success'])}\n")
        f.write(f"  Failed: {len(results['failed'])}\n")
        f.write(f"  Skipped: {len(results['skipped'])}\n")
        f.write(f"  No images: {len(results['no_images'])}\n")
        f.write(f"\nSuccessful rivers:\n")
        for r in results['success']:
            f.write(f"  - {r}\n")
        if results['failed']:
            f.write(f"\nFailed rivers:\n")
            for r, e in results['failed']:
                f.write(f"  - {r}: {e}\n")
        if results['no_images']:
            f.write(f"\nRivers with no Sentinel images:\n")
            for r in results['no_images']:
                f.write(f"  - {r}\n")
    
    print(f"\n  Log saved: {log_path}")
    
    # ==========================================================================
    # STEP 3: Automatic download from Google Drive to local machine
    # ==========================================================================
    if AUTO_DOWNLOAD_FROM_DRIVE and (results['sentinel_tasks'] or results['dem_tasks']):
        print(f"\n{'='*70}")
        print("   AUTOMATIC DOWNLOAD FROM GOOGLE DRIVE")
        print("="*70)
        print(f"\nConfiguration:")
        print(f"  Sentinel download path: {LOCAL_DOWNLOAD_PATH_SENTINEL}")
        print(f"  DEM download path: {LOCAL_DOWNLOAD_PATH_DEM}")
        print(f"  Wait for all tasks: {WAIT_ALL_TASKS_COMPLETE}")
        print(f"  Poll interval: {TASK_POLL_INTERVAL}s")
        print(f"  Max wait time: {MAX_WAIT_HOURS}h")
        print(f"  Delete after download: {DELETE_AFTER_DOWNLOAD}")
        
        try:
            # Authenticate with Google Drive
            drive_service = authenticate_google_drive()
            
            # All task names to wait for
            all_task_names = results['sentinel_tasks'] + results['dem_tasks']
            
            if WAIT_ALL_TASKS_COMPLETE:
                # Wait for all tasks to complete, then download everything
                print(f"\n  Waiting for all {len(all_task_names)} GEE tasks to complete...")
                
                # Wait for completion
                download_results = wait_and_download_gee_tasks(
                    service=drive_service,
                    task_names=all_task_names,
                    drive_folder_name=DRIVE_FOLDER,
                    sentinel_path=LOCAL_DOWNLOAD_PATH_SENTINEL,
                    dem_path=LOCAL_DOWNLOAD_PATH_DEM,
                    poll_interval=TASK_POLL_INTERVAL,
                    max_wait_hours=MAX_WAIT_HOURS,
                    delete_after=DELETE_AFTER_DOWNLOAD
                )
            else:
                # Download files as they complete (progressive download)
                print(f"\n  Starting progressive download (files download as tasks complete)...")
                
                download_results = wait_and_download_gee_tasks(
                    service=drive_service,
                    task_names=all_task_names,
                    drive_folder_name=DRIVE_FOLDER,
                    sentinel_path=LOCAL_DOWNLOAD_PATH_SENTINEL,
                    dem_path=LOCAL_DOWNLOAD_PATH_DEM,
                    poll_interval=TASK_POLL_INTERVAL,
                    max_wait_hours=MAX_WAIT_HOURS,
                    delete_after=DELETE_AFTER_DOWNLOAD
                )
            
            # Download summary
            print(f"\n{'='*70}")
            print("DOWNLOAD COMPLETE")
            print('='*70)
            print(f"\n  📡 Sentinel files: {len(download_results.get('sentinel_files', []))}")
            print(f"     Location: {LOCAL_DOWNLOAD_PATH_SENTINEL}")
            print(f"\n  🗻 DEM files: {len(download_results.get('dem_files', []))}")
            print(f"     Location: {LOCAL_DOWNLOAD_PATH_DEM}")
            
            if download_results.get('failed_tasks', 0) > 0:
                print(f"\n  ⚠ Failed tasks: {download_results['failed_tasks']}")
            
            # Update log with download info
            with open(log_path, 'a') as f:
                f.write(f"\n\n--- Download Results ---\n")
                f.write(f"Sentinel files downloaded: {len(download_results.get('sentinel_files', []))}\n")
                f.write(f"DEM files downloaded: {len(download_results.get('dem_files', []))}\n")
                f.write(f"Failed GEE tasks: {download_results.get('failed_tasks', 0)}\n")
                
        except Exception as e:
            print(f"\n  ✗ Download error: {e}")
            print(f"\n  You can manually download files from Google Drive:")
            print(f"     Drive folder: {DRIVE_FOLDER}/")
            print(f"     - Sentinel/")
            print(f"     - DEM/")
    
    elif not AUTO_DOWNLOAD_FROM_DRIVE:
        print(f"\n  Auto-download disabled. Download files manually from Google Drive:")
        print(f"  Drive folder: {DRIVE_FOLDER}/")
        print(f"    - Sentinel/")
        print(f"    - DEM/")
    
    print(f"\n{'='*70}")
    print("   ALL DONE!")
    print("="*70)
    
    return results


if __name__ == "__main__":
    main()
