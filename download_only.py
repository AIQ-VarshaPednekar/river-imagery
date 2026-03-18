import os
import io
import socket
import pickle
import requests
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request, AuthorizedSession
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import googleapiclient.discovery

# Force requests instead of httplib2
import google.auth.transport.requests
import google_auth_httplib2

socket.setdefaulttimeout(300)  # 5 min timeout

# ── CONFIG ──────────────────────────────────────────────────────────
TOKEN_FILE        = r"C:\Users\My Pc\Documents\river project aiq\drive_token.pickle"
CREDENTIALS_FILE  = r"C:\Users\My Pc\Documents\river project aiq\client_secret.json"
SENTINEL_LOCAL    = r"C:\Users\My Pc\Documents\river project aiq\Imagery_Output\Sentinel"
DEM_LOCAL         = r"C:\Users\My Pc\Documents\river project aiq\Imagery_Output\DEM"
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
# ────────────────────────────────────────────────────────────────────

def get_drive_service():
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

    # Use requests-based HTTP instead of httplib2
    authed_session = AuthorizedSession(creds)
    authed_session.request = lambda method, url, **kwargs: requests.request(
        method, url, headers=kwargs.get('headers', {}),
        data=kwargs.get('body', None), timeout=300
    )

    service = build('drive', 'v3', credentials=creds,
                    requestBuilder=None)
    return service, creds


def download_file_direct(creds, file_id, file_name, local_path, size_mb):
    """Download using requests directly - more reliable than googleapiclient."""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # Get fresh token
    if creds.expired:
        creds.refresh(Request())

    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    headers = {"Authorization": f"Bearer {creds.token}"}

    with requests.get(url, headers=headers, stream=True, timeout=300) as r:
        r.raise_for_status()
        downloaded = 0
        total = int(r.headers.get('content-length', size_mb * 1024 * 1024))

        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):  # 8MB chunks
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    pct = int(downloaded / total * 100) if total else 0
                    print(f"  {pct}% ({downloaded/1024/1024:.1f} MB)", end='\r')

    print(f"  ✓ Downloaded: {file_name}          ")


def list_all_tif_files(creds):
    """Search Drive for Ajay .tif files using requests directly."""
    print("\nSearching Drive for .tif files...")

    if creds.expired:
        creds.refresh(Request())

    files = []
    page_token = None

    while True:
        params = {
            "q": "name contains 'Amba' and name contains '.tif' and trashed=false",
            "spaces": "drive",
            "fields": "nextPageToken, files(id, name, size)",
            "pageSize": 100,
        }
        if page_token:
            params["pageToken"] = page_token

        headers = {"Authorization": f"Bearer {creds.token}"}
        r = requests.get(
            "https://www.googleapis.com/drive/v3/files",
            headers=headers, params=params, timeout=60
        )
        r.raise_for_status()
        data = r.json()
        files.extend(data.get('files', []))
        page_token = data.get('nextPageToken')
        if not page_token:
            break

    return files


def main():
    print("=" * 50)
    print("  DIRECT DRIVE DOWNLOAD")
    print("=" * 50)

    service, creds = get_drive_service()
    print("✓ Authenticated")

    files = list_all_tif_files(creds)
    print(f"Found {len(files)} .tif files in Drive\n")

    for f in files:
        size_mb = int(f.get('size', 0)) / (1024 * 1024)
        print(f"  {f['name']} ({size_mb:.1f} MB)")

    print()

    for f in files:
        name = f['name']
        size_mb = int(f.get('size', 0)) / (1024 * 1024)

        if '_dem' in name.lower():
            local_file = os.path.join(DEM_LOCAL, name)
        else:
            local_file = os.path.join(SENTINEL_LOCAL, name)

        if os.path.exists(local_file):
            local_size = os.path.getsize(local_file) / (1024 * 1024)
            if abs(local_size - size_mb) < 1:
                print(f"⏭ Already exists: {name}")
                continue
            else:
                print(f"⚠ Incomplete, re-downloading: {name} ({local_size:.1f} MB local vs {size_mb:.1f} MB drive)")

        print(f"Downloading: {name} ({size_mb:.1f} MB)...")
        try:
            download_file_direct(creds, f['id'], name, local_file, size_mb)
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            print(f"  → Run script again to retry")

    print("\n" + "=" * 50)
    print("DONE!")
    print(f"Sentinel → {SENTINEL_LOCAL}")
    print(f"DEM      → {DEM_LOCAL}")
    print("=" * 50)


if __name__ == "__main__":
    main()