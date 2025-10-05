import io
import os
import json
import gspread
import tools.dj_set_processor.config as config
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

FOLDER_CACHE = {}


def load_credentials():
    """Load credentials either from GitHub secret (GOOGLE_CREDENTIALS_JSON) or local credentials.json"""
    if os.getenv("GOOGLE_CREDENTIALS_JSON"):
        creds_dict = json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON"))
        return service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/spreadsheets",
            ],
        )
    else:
        return service_account.Credentials.from_service_account_file(
            "credentials.json",
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/spreadsheets",
            ],
        )


def get_drive_service():
    """Return a Drive API client"""
    config.logging.debug("Loading credentials and initializing Drive service")
    creds = load_credentials()
    service = build("drive", "v3", credentials=creds)
    config.logging.debug("Drive service initialized")
    return service


def get_gspread_client():
    """Return a gspread client (not Google API resource)"""
    config.logging.debug("Loading credentials and initializing gspread client")
    creds = load_credentials()
    gc = gspread.authorize(creds)  # <-- THIS is the difference
    config.logging.debug("gspread client initialized")
    return gc


def get_or_create_folder(parent_folder_id: str, name: str, drive_service) -> str:
    """Returns the folder ID for a subfolder under parent_folder_id with the given name.
    Creates it if it doesn't exist, using an in-memory cache to avoid duplication."""
    cache_key = f"{parent_folder_id}/{name}"
    if cache_key in FOLDER_CACHE:
        return FOLDER_CACHE[cache_key]

    # Search for existing folder
    query = (
        f"'{parent_folder_id}' in parents and "
        f"name = '{name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    )
    response = (
        drive_service.files()
        .list(
            q=query,
            spaces="drive",
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    folders = response.get("files", [])
    if folders:
        print(f"📁 Found existing folder '{name}' under parent {parent_folder_id}")
        folder_id = folders[0]["id"]
    else:
        print(f"📁 Creating new folder '{name}' under parent {parent_folder_id}")
        folder_metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id],
        }
        folder = (
            drive_service.files()
            .create(body=folder_metadata, fields="id", supportsAllDrives=True)
            .execute()
        )
        folder_id = folder["id"]

    FOLDER_CACHE[cache_key] = folder_id
    return folder_id


def upload_to_drive(drive, filepath, parent_id):
    config.logging.debug(f"Uploading file '{filepath}' to Drive folder ID '{parent_id}'")
    file_metadata = {
        "name": os.path.basename(filepath),
        "parents": [parent_id],
        "mimeType": "application/vnd.google-apps.spreadsheet",
    }
    media = MediaFileUpload(filepath, mimetype="text/csv")
    uploaded = (
        drive.files()
        .create(body=file_metadata, media_body=media, fields="id", supportsAllDrives=True)
        .execute()
    )
    config.logging.info(f"📄 Uploaded to Drive as Google Sheet: {filepath}")
    config.logging.debug(f"Uploaded file ID: {uploaded['id']}")

    # After uploading, use gspread to open the sheet and check for 'sep=' in first row of all worksheets
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(uploaded["id"])
    for sheet in spreadsheet.worksheets():
        first_row = sheet.row_values(1)
        if first_row and first_row[0].strip().lower().startswith("sep="):
            sheet.delete_rows(1)

    return uploaded["id"]


def list_files_in_drive_folder(drive, folder_id):
    config.logging.debug(f"Listing files in Drive folder ID: {folder_id}")
    query = f"'{folder_id}' in parents and trashed = false"
    files = []
    page_token = None
    while True:
        response = (
            drive.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken", None)
        if page_token is None:
            break
    config.logging.debug(f"Found {len(files)} files in folder ID: {folder_id}")
    return files


def download_file(drive, file_id, destination_path):
    config.logging.debug(f"Downloading file ID '{file_id}' to '{destination_path}'")
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(destination_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()
    config.logging.info(f"⬇️ Downloaded file to {destination_path}")


def apply_sheet_formatting(sheet):
    # Set font size and alignment for entire sheet
    sheet.format("A:Z", {"textFormat": {"fontSize": 10}, "horizontalAlignment": "LEFT"})

    # Freeze header row
    sheet.freeze(rows=1)

    # Bold the header row
    sheet.format("1:1", {"textFormat": {"bold": True}})

    # Auto resize first 10 columns (A-J)
    sheet.spreadsheet.batch_update(
        {
            "requests": [
                {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet._properties["sheetId"],
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": 10,
                        }
                    }
                }
            ]
        }
    )


def apply_formatting_to_sheet(spreadsheet_id):
    config.logging.debug(f"Applying formatting to spreadsheet ID: {spreadsheet_id}")
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(spreadsheet_id)
        sheet = sh.sheet1
        config.logging.debug("Opened sheet for formatting")

        # Get all values to determine range size
        values = sheet.get_all_values()
        if not values or len(values) == 0 or len(values[0]) == 0:
            config.logging.warning("Sheet is empty, skipping formatting")
            return

        apply_sheet_formatting(sheet)

        config.logging.info("✅ Formatting applied successfully")
    except Exception as e:
        config.logging.error(f"Error applying formatting: {e}")
