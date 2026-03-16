# River Imagery Download Pipeline

Downloads Sentinel-2 satellite imagery and SRTM DEM data for Indian rivers using
Google Earth Engine, exports to Google Drive, and merges tiles locally.

---

## Project Structure

```
river project aiq/
├── updatedriverscode.py       # Main script: GEE export + Drive download
├── merge.py                   # Merges downloaded tiles into single GeoTIFF
├── download_only.py           # Standalone Drive downloader (bypass GEE)
├── client_secret.json         # ⚠ Google OAuth credentials (DO NOT COMMIT)
├── drive_token.pickle         # ⚠ Saved Drive auth token (DO NOT COMMIT)
├── river_env/                 # Python virtual environment
└── Imagery_Output/
    ├── Sentinel/              # Downloaded Sentinel-2 tiles (.tif)
    ├── DEM/                   # Downloaded DEM tiles (.tif)
    └── Sentinel_Merged/       # Final merged GeoTIFFs per river
```

---

## Setup Instructions

### 1. Create and activate virtual environment

```bash
cd "C:\Users\My Pc\Documents\river project aiq"
python -m venv river_env
river_env\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Authenticate Google Earth Engine

```bash
earthengine authenticate
```

- This opens a browser. Log in with your Google account.
- GEE Project ID used: `plucky-sight-423703-k5`

### 4. Set up Google Drive OAuth credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project → Enable **Google Drive API**
3. Create **OAuth 2.0 Client ID** (Desktop app type)
4. Download JSON → rename to `client_secret.json` → place in project folder
5. Add your Google account as a **Test User** under OAuth consent screen
6. On first run, a browser will open to authorize — token saved as `drive_token.pickle`

---

## Configuration (updatedriverscode.py)

Edit these variables at the top of `updatedriverscode.py`:

| Variable | Description | Example |
|---|---|---|
| `SHAPEFILE_PATH` | Path to river shapefile | `r"C:\...\River_India_Final.shp"` |
| `SPECIFIC_RIVERS` | List of rivers to process | `["Ajay"]` or `[]` for all |
| `BUFFER_DISTANCE` | Buffer around river in meters | `10000` (10 km) |
| `RESOLUTION` | Output pixel size in meters | `10` (Sentinel native) |
| `START_DATE` | Imagery date range start | `"2024-01-01"` |
| `END_DATE` | Imagery date range end | `"2024-12-31"` |
| `MAX_CLOUD_COVER` | Max cloud % filter | `10` |
| `DRIVE_FOLDER` | Google Drive folder name | `"River_Imagery_Batch"` |
| `OUTPUT_BASE_FOLDER` | Local output base path | `r"C:\...\Imagery_Output"` |
| `GOOGLE_CREDENTIALS_FILE` | Path to client_secret.json | `r"C:\...\client_secret.json"` |
| `TOKEN_FILE` | Path to drive_token.pickle | `r"C:\...\drive_token.pickle"` |
| `GEE_PROJECT` | Your GEE Cloud Project ID | `"plucky-sight-423703-k5"` |

**Shapefile column name:** The shapefile uses column `rivname` for river names.
This is renamed to `name` in the script:
```python
all_rivers_gdf = all_rivers_gdf.rename(columns={"rivname": "name"})
```
If your shapefile uses a different column name, update this line.

---

## Running the Pipeline

### Step 1: Export imagery from GEE + auto-download

```bash
python updatedriverscode.py
```

**What it does:**
1. Reads river geometries from shapefile
2. Creates 10km buffer around each river
3. Submits GEE export tasks (Sentinel-2 median composite + SRTM DEM)
4. GEE exports files to Google Drive folder `River_Imagery_Batch/Sentinel` and `River_Imagery_Batch/DEM`
5. Monitors task completion and auto-downloads to local folders

**GEE exports to Drive as:**
- `River_Imagery_Batch/Sentinel/{river}_sentinel-XXXXXXXXXX-XXXXXXXXXX.tif` (may be multiple tiles)
- `River_Imagery_Batch/DEM/{river}_dem.tif`

**Note:** Large rivers (e.g. Yamuna) may exceed Google Drive free storage (15GB).
Consult your senior about appropriate buffer distance before scaling to all 150 rivers.

---

### Step 2 (if download fails): Direct Drive download

If auto-download fails or times out, use the standalone downloader:

```bash
python download_only.py
```

This searches Drive directly by filename and downloads with `requests` (more reliable
for large files than the default httplib2 transport).

---

### Step 3: Merge tiles into single GeoTIFF

```bash
python merge.py
```

**What it does:**
- Reads all `.tif` tiles from `Imagery_Output/Sentinel/`
- Groups tiles by river name (splits on `-000` in filename)
- Merges tiles using memory-efficient windowed writing (2048 rows at a time)
- Outputs to `Imagery_Output/Sentinel_Merged/{river}_sentinel_merged.tif`

**Output format:**
- Float32 GeoTIFF (standard for satellite imagery analysis)
- LZW compressed
- Tiled (512×512 blocks) for fast GIS access
- BigTIFF enabled (supports files >4GB)

---

## Imagery Details

### Sentinel-2 (Sentinel-2 SR Harmonized)
- **Collection:** `COPERNICUS/S2_SR_HARMONIZED`
- **Bands exported:** B1, B2, B3, B4, B5, B6, B7, B8, B8A, B9, B11, B12
- **Processing:** Median composite of all images in date range with <10% cloud cover
- **Resolution:** 10m

### DEM (SRTM)
- **Collection:** `USGS/SRTMGL1_003`
- **Resolution:** ~30m (resampled to 10m in export)

---

## Known Issues & Solutions

| Issue | Cause | Fix |
|---|---|---|
| `Drive folder not found: River_Imagery_Batch` | GEE creates folders with literal slash in name e.g. `River_Imagery_Batch/Sentinel` | Fixed in script — searches directly by full folder name |
| `TimeoutError: [WinError 10060]` | Network timeout on large file download | Re-run script — it skips already downloaded files |
| `Unable to allocate 25 GiB` during merge | rasterio loads entire mosaic into RAM | Fixed — use windowed merge in merge.py |
| `Not recognized as supported file format` | Corrupted/incomplete download | Delete the file and re-download |
| `Not enough space in Google Drive (need 8.3GB)` | Large river + 10km buffer + 10m resolution | Reduce buffer or resolution; discuss with senior |

---

## Tested Rivers

| River | Buffer Area | Sentinel Tiles | Sentinel Size | DEM Size | Status |
|---|---|---|---|---|---|
| Ajay | 6,178 km² | 8 tiles | ~1.7 GB | 18.9 MB | ✅ Complete |
| Yamuna | 26,666 km² | — | ~8.3 GB | — | ❌ Drive space exceeded |

---

## Notes for Scaling to All 150 Rivers

- Current Google Drive free space: ~15 GB — insufficient for all rivers at 10km/10m
- Options: reduce buffer (e.g. 1km), reduce resolution (30m), or use paid Drive storage
- Set `SPECIFIC_RIVERS = []` to process all rivers
- Set `MAX_CONCURRENT_TASKS = 100` (GEE allows up to 3000 concurrent tasks)
- Recommend processing in batches of 10–20 rivers at a time
- **Confirm buffer distance and resolution with your senior before scaling**