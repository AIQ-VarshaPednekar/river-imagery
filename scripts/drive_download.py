import os
import pickle
import requests
import socket
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

socket.setdefaulttimeout(300)

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Overridden at runtime by run_step2.py from config.json
TOKEN_FILE            = ""
CREDENTIALS_FILE      = ""
SENTINEL_LOCAL        = ""
DEM_LOCAL             = ""
DRIVE_SENTINEL_FOLDER = "River_Imagery_Batch/Sentinel"
DRIVE_DEM_FOLDER      = "River_Imagery_Batch/DEM"


def get_creds():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as f:
            creds = pickle.load(f)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, 'wb') as f:
            pickle.dump(creds, f)
    return creds


def auth_headers(creds):
    """Always return a fresh bearer token header."""
    if creds.expired:
        creds.refresh(Request())
    return {"Authorization": f"Bearer {creds.token}"}


def find_folder_id(creds, folder_name):
    """Find Drive folder ID by exact name."""
    print(f"  Searching for Drive folder: '{folder_name}' ...")
    params = {
        "q": f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
        "fields": "files(id, name)",
        "pageSize": 10,
    }
    r = requests.get(
        "https://www.googleapis.com/drive/v3/files",
        headers=auth_headers(creds), params=params, timeout=60
    )
    r.raise_for_status()
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
    """List all files (non-folders) directly inside a Drive folder."""
    files = []
    page_token = None
    while True:
        params = {
            "q": (f"'{folder_id}' in parents "
                  f"and mimeType != 'application/vnd.google-apps.folder' "
                  f"and trashed = false"),
            "fields": "nextPageToken, files(id, name, size)",
            "pageSize": 100,
        }
        if page_token:
            params["pageToken"] = page_token
        r = requests.get(
            "https://www.googleapis.com/drive/v3/files",
            headers=auth_headers(creds), params=params, timeout=60
        )
        r.raise_for_status()
        data = r.json()
        files.extend(data.get('files', []))
        page_token = data.get('nextPageToken')
        if not page_token:
            break
    return files


def download_file(creds, file_id, file_name, local_path, drive_size_bytes):
    """
    Download with resume support.
    - Local file exists and matches Drive size  → skip (already complete)
    - Local file exists and is smaller          → resume from that byte offset
    - Local file does not exist or is larger    → fresh download
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    local_size = os.path.getsize(local_path) if os.path.exists(local_path) else 0

    # Already complete
    if local_size == drive_size_bytes and drive_size_bytes > 0:
        print(f"  ⏭  SKIPPED (already complete): {file_name} ({local_size/1024/1024:.1f} MB)")
        return "skipped"

    # Decide resume offset
    if 0 < local_size < drive_size_bytes:
        resume_from = local_size
        remaining = drive_size_bytes - local_size
        print(f"  ↻  RESUMING from {local_size/1024/1024:.1f} MB: {file_name}")
        print(f"     ({remaining/1024/1024:.1f} MB remaining)")
    else:
        resume_from = 0
        print(f"  ↓  DOWNLOADING: {file_name} ({drive_size_bytes/1024/1024:.1f} MB)")

    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    headers = auth_headers(creds)
    if resume_from > 0:
        headers["Range"] = f"bytes={resume_from}-"

    with requests.get(url, headers=headers, stream=True, timeout=300) as r:
        r.raise_for_status()
        mode = 'ab' if resume_from > 0 else 'wb'
        downloaded = resume_from
        with open(local_path, mode) as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    pct = int(downloaded / drive_size_bytes * 100) if drive_size_bytes else 0
                    mb_done = downloaded / 1024 / 1024
                    mb_total = drive_size_bytes / 1024 / 1024
                    print(f"     Progress: {pct:3d}% | {mb_done:7.1f} / {mb_total:.1f} MB", end='\r')

    print(f"  ✓  DONE: {file_name}" + " " * 50)  # Padding to clear the progress line
    return "downloaded"


def process_folder(creds, drive_folder_name, local_dir):
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
        drive_size_bytes = int(f.get('size', 0))
        local_path       = os.path.join(local_dir, name)

        try:
            result = download_file(creds, f['id'], name, local_path, drive_size_bytes)
            counts[result] += 1
        except Exception as e:
            print(f"\n  ✗  Failed: {name}")
            print(f"     Reason: {e}")
            print(f"     → Re-run the script to resume this file")
            counts["failed"] += 1

    print(f"\n  Summary → ✓ downloaded: {counts['downloaded']}  "
          f"⏭ skipped: {counts['skipped']}  "
          f"✗ failed: {counts['failed']}")
    return counts["failed"]


def main():
    print("=" * 55)
    print("  DRIVE FOLDER DOWNLOADER  (with resume support)")
    print("=" * 55)

    creds = get_creds()
    print("✓ Authenticated\n")

    total_failed = 0
    total_failed += process_folder(creds, DRIVE_SENTINEL_FOLDER, SENTINEL_LOCAL)
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
    main()