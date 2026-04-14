"""
drive_download.py — Google Drive Downloader with Resume Support
================================================================
PURPOSE:
  Download Sentinel-2 and DEM GeoTIFF files from Google Drive to local disk.

  After Google Earth Engine finishes exporting satellite imagery,
  the files appear in these Google Drive folders:
    • River_Imagery_Batch/Sentinel/  — Sentinel-2 tiles (e.g. Ganga_sentinel-000.tif)
    • River_Imagery_Batch/DEM/       — DEM tiles (e.g. Ganga_dem.tif)

  This script authenticates with Drive (OAuth2), lists the files in those folders,
  and downloads each one to local disk with:
    • SKIP support:   If local file size == Drive file size → already complete, skip.
    • RESUME support: If local file size < Drive file size → use HTTP Range header
                      to continue the download from where it stopped.

HOW AUTHENTICATION WORKS:
  Google Drive uses OAuth2 (user-based authentication, not service account).
  The flow:
    1. First run: Opens a browser window → user grants permission → token saved to drive_token.pickle
    2. Subsequent runs: Load token from drive_token.pickle → auto-refresh if expired
    3. All API calls use Bearer token in Authorization header

  Why OAuth2 instead of a service account?
    The GEE export saves files to THE USER'S personal Google Drive.
    Service accounts have their own Drive space (not accessible to the user's GEE exports).
    OAuth2 acts as the user, so it can see the user's Drive.

CONFIG:
  All variables below are OVERRIDDEN at runtime by run_step2.py using values
  from config.json. Do NOT hardcode sensitive paths here — they come from config.

CALLED BY:
  run_step2.py → mod.main() after overriding these globals

CHUNK SIZE:
  Files are downloaded in 8 MB chunks (8 * 1024 * 1024 bytes).
  This allows progress tracking and uses less RAM than downloading the entire file at once.
  GeoTIFFs can be 500 MB – 5 GB each, so streaming is essential.
"""

import os             # os.path.exists(), os.makedirs(), os.path.getsize()
import pickle         # Serialize/deserialise the OAuth2 token to/from drive_token.pickle
import requests       # HTTP library for Drive API calls and file downloads (streaming)
import socket         # socket.setdefaulttimeout() — prevent indefinite hangs on slow network
from google_auth_oauthlib.flow import InstalledAppFlow  # OAuth2 browser-based login flow
from google.auth.transport.requests import Request       # Needed to refresh expired tokens

# ── Network timeout ────────────────────────────────────────────────────────────
# Set a global socket timeout of 5 minutes (300 seconds).
# Without this, a stalled download could hang indefinitely.
# Large GeoTIFF downloads can be slow — 300s gives enough runway per TCP chunk.
socket.setdefaulttimeout(300)

# ── OAuth2 Scopes ──────────────────────────────────────────────────────────────
# 'drive.readonly' = list folders + download files, but cannot create/delete.
# We use readonly because we only DOWNLOAD; we never upload or modify Drive files.
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# ── Configuration globals (overridden by run_step2.py at runtime) ──────────────
TOKEN_FILE            = ""   # Path to drive_token.pickle (cached OAuth token)
CREDENTIALS_FILE      = ""   # Path to client_secret.json (OAuth app credentials)
SENTINEL_LOCAL        = ""   # Local folder to save Sentinel tiles into
DEM_LOCAL             = ""   # Local folder to save DEM tiles into
DRIVE_SENTINEL_FOLDER = "River_Imagery_Batch/Sentinel"  # Drive folder name for Sentinel
DRIVE_DEM_FOLDER      = "River_Imagery_Batch/DEM"       # Drive folder name for DEM


def get_creds():
    """Load OAuth2 credentials from disk; refresh or re-authenticate if needed.

    FLOW:
      1. Try to load from TOKEN_FILE (drive_token.pickle):
           - pickle.load() deserialises the stored google.oauth2.credentials.Credentials object
      2. If token is valid → return it directly (no network call needed)
      3. If token is expired but has a refresh_token → refresh silently (one HTTP call)
      4. If no token or refresh fails → launch browser OAuth flow:
           - Opens localhost:0 (random port) for the OAuth redirect
           - User sees consent screen in browser
           - Saves the new token to TOKEN_FILE for next time

    Why pickle instead of JSON?
      The google.oauth2.credentials.Credentials object includes methods and
      non-serialisable types. pickle handles this natively. JSON would require
      manual serialisation/deserialisation of each field.

    Returns:
        google.oauth2.credentials.Credentials — valid access token
    """
    creds = None

    # Load existing token from disk
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as f:
            creds = pickle.load(f)   # Deserialise the credentials object

    if not creds or not creds.valid:   # No token, or token is expired/invalid
        if creds and creds.expired and creds.refresh_token:
            # Token expired but we have a refresh token → renew silently
            creds.refresh(Request())   # Makes one POST to Google's token endpoint
        else:
            # No token at all → full browser OAuth flow
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)  # port=0 = OS picks an available port

        # Save refreshed/new credentials so we don't re-authenticate next time
        with open(TOKEN_FILE, 'wb') as f:
            pickle.dump(creds, f)

    return creds


def auth_headers(creds):
    """Build Authorization header dict with a fresh Bearer token.

    WHY check creds.expired before every request?
      Access tokens expire after 1 hour. If the download session lasts longer
      (e.g. many large files), the token will expire mid-download.
      By refreshing before each API call (not just each file), we ensure
      the headers are always valid.

    Args:
        creds: google.oauth2.credentials.Credentials

    Returns:
        dict — {"Authorization": "Bearer <access_token>"}
    """
    if creds.expired:
        creds.refresh(Request())   # Refresh silently using the refresh token
    return {"Authorization": f"Bearer {creds.token}"}


def find_folder_id(creds, folder_name):
    """Find the Drive folder ID for a given folder name.

    Google Drive uses opaque IDs (not paths) to identify files and folders.
    We must query the Drive API to get the ID from the folder name.

    WHY search by name instead of hardcoding IDs?
      Folder IDs change if someone deletes+recreates the folder.
      Searching by name is more resilient and readable in config.

    API Query syntax (Drive API query language):
      name = 'Sentinel'                         — exact name match
      mimeType = 'application/vnd.google-apps.folder'  — folders only (not files)
      trashed = false                           — exclude deleted items

    If multiple folders have the same name, we use the first result.
    GEE always creates the folder at the expected path, so this is reliable.

    Args:
        creds:       OAuth2 credentials
        folder_name: The LAST segment of the folder name (not full path).
                     e.g. "Sentinel" (not "River_Imagery_Batch/Sentinel")

    Returns:
        str — the Drive folder ID

    Raises:
        FileNotFoundError: if no folder with that name is found
    """
    print(f"  Searching for Drive folder: '{folder_name}' ...")
    params = {
        "q": (f"name = '{folder_name}' "
              f"and mimeType = 'application/vnd.google-apps.folder' "
              f"and trashed = false"),
        "fields": "files(id, name)",     # Only fetch id and name (less data)
        "pageSize": 10,
    }
    r = requests.get(
        "https://www.googleapis.com/drive/v3/files",
        headers=auth_headers(creds), params=params, timeout=60
    )
    r.raise_for_status()   # Raise exception for HTTP 4xx/5xx errors
    results = r.json().get('files', [])
    if not results:
        raise FileNotFoundError(
            f"\n  ERROR: Folder not found in Drive: '{folder_name}'\n"
            f"  → Open the folder in Drive, copy the ID from the URL,\n"
            f"    and hard-code it in SENTINEL_FOLDER_ID / DEM_FOLDER_ID at the top."
        )
    folder_id = results[0]['id']
    print(f"  ✓ Found → id: {folder_id}")
    return folder_id


def list_files_in_folder(creds, folder_id):
    """List all non-folder files directly inside a specific Drive folder.

    Uses Drive API v3 with pagination support.
    Why pagination?
      The Drive API returns at most 100 items per request (pageSize limit).
      Rivers with many tiles (large rivers split into many -000, -001, ... files)
      might exceed this. We loop through pages using nextPageToken until exhausted.

    API Query:
      '{folder_id}' in parents        — file is directly inside this folder
      mimeType != 'application/vnd.google-apps.folder'  — exclude sub-folders
      trashed = false                 — exclude deleted items

    Returns:
        list[dict] — each dict has keys: 'id', 'name', 'size'
    """
    files = []
    page_token = None

    while True:
        params = {
            "q": (f"'{folder_id}' in parents "
                  f"and mimeType != 'application/vnd.google-apps.folder' "
                  f"and trashed = false"),
            "fields": "nextPageToken, files(id, name, size)",
            "pageSize": 100,   # Max allowed by Drive API
        }
        if page_token:
            params["pageToken"] = page_token   # Continue from previous page

        r = requests.get(
            "https://www.googleapis.com/drive/v3/files",
            headers=auth_headers(creds), params=params, timeout=60
        )
        r.raise_for_status()
        data = r.json()
        files.extend(data.get('files', []))

        page_token = data.get('nextPageToken')
        if not page_token:
            break   # No more pages → all files collected

    return files


def download_file(creds, file_id, file_name, local_path, drive_size_bytes):
    """Download a single file from Google Drive with skip + resume logic.

    LOGIC:
      1. If local file exists AND its size == Drive size → already complete → skip
      2. If local file exists AND its size < Drive size → partial download → resume
      3. Otherwise → fresh download from scratch

    WHY compare sizes?
      We can't compare checksums easily without reading the entire Drive file.
      File size is available from Drive metadata (drive_size_bytes parameter).
      Size equality is a reliable proxy for completeness for GeoTIFF files
      (GEE never produces partial/truncated files in Drive).

    RESUME implementation:
      HTTP Range header: "Range: bytes=<offset>-"
      This tells the server to start sending from byte <offset>.
      We open the local file in 'ab' (append binary) mode so the new bytes
      are appended to the existing partial content.

    STREAMING:
      We use requests.get(..., stream=True) and iter_content(chunk_size=8MB).
      This keeps memory usage at ~8MB regardless of file size.

    Args:
        creds:            OAuth2 credentials
        file_id:          Drive file ID
        file_name:        Display name (for logging only)
        local_path:       Absolute local file path to write to
        drive_size_bytes: File size in Drive (from API metadata)

    Returns:
        str — "skipped" | "downloaded"
    """
    # Ensure local directory exists (creates parent folders if needed)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # Check what we already have on disk
    local_size = os.path.getsize(local_path) if os.path.exists(local_path) else 0

    # ── Already complete → skip ────────────────────────────────────────────────
    if local_size == drive_size_bytes and drive_size_bytes > 0:
        print(f"  ⏭  SKIPPED (already complete): {file_name} ({local_size/1024/1024:.1f} MB)")
        return "skipped"

    # ── Decide resume vs fresh ─────────────────────────────────────────────────
    if 0 < local_size < drive_size_bytes:
        # Partial file exists → resume from where we left off
        resume_from = local_size
        remaining = drive_size_bytes - local_size
        print(f"  ↻  RESUMING from {local_size/1024/1024:.1f} MB: {file_name}")
        print(f"     ({remaining/1024/1024:.1f} MB remaining)")
    else:
        # Either no local file, or local is larger (corrupted) → fresh download
        resume_from = 0
        print(f"  ↓  DOWNLOADING: {file_name} ({drive_size_bytes/1024/1024:.1f} MB)")

    # ── Build request headers ─────────────────────────────────────────────────
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    # alt=media tells Drive API to return the file content, not the metadata JSON
    headers = auth_headers(creds)
    if resume_from > 0:
        headers["Range"] = f"bytes={resume_from}-"   # HTTP range request for resume

    # ── Stream download ────────────────────────────────────────────────────────
    with requests.get(url, headers=headers, stream=True, timeout=300) as r:
        r.raise_for_status()
        mode = 'ab' if resume_from > 0 else 'wb'   # Append for resume, write for fresh
        downloaded = resume_from   # Track total bytes received so far

        with open(local_path, mode) as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):   # 8 MB chunks
                if chunk:   # filter out keep-alive empty chunks
                    f.write(chunk)
                    downloaded += len(chunk)

                    # Live progress display (overwriting the same line with \r)
                    pct = int(downloaded / drive_size_bytes * 100) if drive_size_bytes else 0
                    mb_done  = downloaded / 1024 / 1024
                    mb_total = drive_size_bytes / 1024 / 1024
                    print(f"     Progress: {pct:3d}% | {mb_done:7.1f} / {mb_total:.1f} MB", end='\r')

    print(f"  ✓  DONE: {file_name}" + " " * 50)  # Extra spaces clear the progress line
    return "downloaded"


def process_folder(creds, drive_folder_name, local_dir):
    """Download all files from one Drive folder to one local directory.

    This is the main download loop:
      1. Find the Drive folder ID by name
      2. List all files in the folder
      3. For each file: call download_file() with resume/skip logic
      4. Return failure count so the caller (main()) can report partial failures

    Why return failure count instead of raising?
      A few failed files shouldn't abort the entire download.
      The user can re-run Step 2 to resume any failed files.

    Args:
        creds:             OAuth2 credentials
        drive_folder_name: Drive folder name to download from (last path segment)
        local_dir:         Local directory to save files into

    Returns:
        int — number of files that failed to download
    """
    print(f"\n{'='*55}")
    print(f"  FOLDER : {drive_folder_name}")
    print(f"  LOCAL  : {local_dir}")
    print(f"{'='*55}")

    folder_id = find_folder_id(creds, drive_folder_name)
    files = list_files_in_folder(creds, folder_id)
    print(f"  {len(files)} file(s) found in Drive folder\n")

    counts = {"downloaded": 0, "skipped": 0, "failed": 0}

    for f in files:
        name             = f['name']
        drive_size_bytes = int(f.get('size', 0))   # Drive size may be string or absent
        local_path       = os.path.join(local_dir, name)

        try:
            result = download_file(creds, f['id'], name, local_path, drive_size_bytes)
            counts[result] += 1   # Increment "downloaded" or "skipped" counter
        except Exception as e:
            print(f"\n  ✗  Failed: {name}")
            print(f"     Reason: {e}")
            print(f"     → Re-run the script to resume this file")
            counts["failed"] += 1

    print(f"\n  Summary → ✓ downloaded: {counts['downloaded']}  "
          f"⏭ skipped: {counts['skipped']}  "
          f"✗ failed: {counts['failed']}")
    return counts["failed"]   # Caller uses this to decide exit code


def main():
    """Top-level entry point: authenticate and download both Sentinel and DEM folders.

    CALLED BY:
      run_step2.py — after overriding module globals with config.json values.
      Can also be run directly (python drive_download.py) with hardcoded default paths.

    If any files fail, the function prints a "re-run to resume" message.
    Does NOT call sys.exit(1) on partial failure — lets the runner decide.
    """
    print("=" * 55)
    print("  DRIVE FOLDER DOWNLOADER  (with resume support)")
    print("=" * 55)

    # Authenticate once; the same credentials object is reused for all downloads.
    # get_creds() handles token loading, refreshing, and interactive login.
    creds = get_creds()
    print("✓ Authenticated\n")

    total_failed = 0
    # Download Sentinel tiles (uses DRIVE_SENTINEL_FOLDER and SENTINEL_LOCAL)
    total_failed += process_folder(creds, DRIVE_SENTINEL_FOLDER, SENTINEL_LOCAL)
    # Download DEM tiles (uses DRIVE_DEM_FOLDER and DEM_LOCAL)
    total_failed += process_folder(creds, DRIVE_DEM_FOLDER,      DEM_LOCAL)

    print("\n" + "=" * 55)
    if total_failed == 0:
        print("  ALL DONE ✓")
    else:
        print(f"  DONE — {total_failed} file(s) failed. Re-run to resume them.")
    print(f"  Sentinel → {SENTINEL_LOCAL}")
    print(f"  DEM      → {DEM_LOCAL}")
    print("=" * 55)


if __name__ == "__main__":
    # Direct execution (not via run_step2.py) — uses placeholder globals above.
    # Useful for manual testing with hardcoded paths.
    main()