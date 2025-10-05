import os
import re
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2.service_account import Credentials
import io
import logging
import gspread

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

# === CONFIGURATION ===
CSV_SOURCE_FOLDER_ID = "1t4d_8lMC3ZJfSyainbpwInoDta7n69hC"
DJ_SETS_FOLDER_ID = "1A0tKQ2DBXI1Bt9h--olFwnBNne3am-rL"
SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = "credentials.json"

FOLDER_CACHE = {}


# === HELPERS ===
def extract_year_from_filename(filename):
    logging.debug(f"extract_year_from_filename called with filename: {filename}")
    match = re.match(r"(\d{4})[-_]", filename)
    year = match.group(1) if match else None
    logging.debug(f"Extracted year: {year} from filename: {filename}")
    return year


def normalize_csv(file_path):
    logging.debug(f"normalize_csv called with file_path: {file_path} - reading file")
    with open(file_path, "r") as f:
        lines = f.readlines()
    cleaned_lines = [re.sub(r"\s+", " ", line).strip() for line in lines if line.strip()]
    logging.debug(f"Lines after cleaning: {len(cleaned_lines)}")
    with open(file_path, "w") as f:
        f.write("\n".join(cleaned_lines))
    logging.info(f"✅ Normalized: {file_path}")


def get_drive_service():
    logging.debug("Loading credentials and initializing Drive service")
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build("drive", "v3", credentials=creds)
    logging.debug("Drive service initialized")
    return service


def get_gspread_client():
    logging.debug("Loading credentials and initializing gspread client")
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    logging.debug("gspread client initialized")
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
    logging.debug(f"Uploading file '{filepath}' to Drive folder ID '{parent_id}'")
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
    logging.info(f"📄 Uploaded to Drive as Google Sheet: {filepath}")
    logging.debug(f"Uploaded file ID: {uploaded['id']}")

    # After uploading, use gspread to open the sheet and check for 'sep=' in first row of all worksheets
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(uploaded["id"])
    for sheet in spreadsheet.worksheets():
        first_row = sheet.row_values(1)
        if first_row and first_row[0].strip().lower().startswith("sep="):
            sheet.delete_rows(1)

    return uploaded["id"]


def list_files_in_drive_folder(drive, folder_id):
    logging.debug(f"Listing files in Drive folder ID: {folder_id}")
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
    logging.debug(f"Found {len(files)} files in folder ID: {folder_id}")
    return files


def download_file(drive, file_id, destination_path):
    logging.debug(f"Downloading file ID '{file_id}' to '{destination_path}'")
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(destination_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()
    logging.info(f"⬇️ Downloaded file to {destination_path}")


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
    logging.debug(f"Applying formatting to spreadsheet ID: {spreadsheet_id}")
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(spreadsheet_id)
        sheet = sh.sheet1
        logging.debug("Opened sheet for formatting")

        # Get all values to determine range size
        values = sheet.get_all_values()
        if not values or len(values) == 0 or len(values[0]) == 0:
            logging.warning("Sheet is empty, skipping formatting")
            return

        apply_sheet_formatting(sheet)

        logging.info("✅ Formatting applied successfully")
    except Exception as e:
        logging.error(f"Error applying formatting: {e}")


# === MAIN ===
def main():
    logging.info("Starting main process")
    drive = get_drive_service()

    files = list_files_in_drive_folder(drive, CSV_SOURCE_FOLDER_ID)
    logging.info(f"Found {len(files)} files in source folder")
    for file_metadata in files:
        filename = file_metadata["name"]
        logging.debug(f"Processing file: {filename}")
        if not filename.endswith(".csv"):
            logging.debug(f"Skipping non-csv file: {filename}")
            continue

        file_id = file_metadata["id"]
        year = extract_year_from_filename(filename)
        if not year:
            logging.warning(f"⚠️ Skipping unrecognized filename format: {filename}")
            continue

        logging.info(f"\n🚧 Processing: {filename}")
        temp_path = os.path.join("/tmp", filename)
        download_file(drive, file_id, temp_path)
        normalize_csv(temp_path)
        logging.info(f"Downloaded and normalized file: {filename}")

        # Get or create year folder in Drive
        year_folder_id = get_or_create_folder(DJ_SETS_FOLDER_ID, year, drive)

        # Upload cleaned CSV as Google Sheet
        try:
            sheet_id = upload_to_drive(drive, temp_path, year_folder_id)
            logging.debug(f"Uploaded sheet ID: {sheet_id}")

            # Apply formatting only if sheet was populated with valid data
            apply_formatting_to_sheet(sheet_id)

            # Commented out to prevent deletion of original files during testing
            # drive.files().delete(fileId=file_id, supportsAllDrives=True).execute()
            # logging.info(f"🗑️ Deleted original file from Drive: {filename}")
        except Exception as e:
            logging.error(f"❌ Failed to upload or format {filename}: {e}")
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)


if __name__ == "__main__":
    main()
