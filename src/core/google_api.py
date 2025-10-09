import io
import os
import json
import gspread
from typing import Any, List, Dict
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
import tools.dj_set_processor.helpers as helpers
from core import logger as log

log = log.get_logger()

FOLDER_CACHE = {}


def load_credentials():
    """Load credentials either from GitHub secret (GOOGLE_CREDENTIALS_JSON) or local credentials.json.
    If GOOGLE_CREDENTIALS_JSON is set but contains invalid JSON or is not a dict, logs a warning and falls back to credentials.json.
    """
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    SCOPES = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    if creds_json:
        try:
            creds_dict = json.loads(creds_json)
            if not isinstance(creds_dict, dict):
                log.warning(
                    "GOOGLE_CREDENTIALS_JSON did not decode to a dictionary. Falling back to credentials.json."
                )
                raise ValueError("Decoded JSON is not a dict")
            return service_account.Credentials.from_service_account_info(
                creds_dict,
                scopes=SCOPES,
            )
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            log.warning(
                f"Invalid GOOGLE_CREDENTIALS_JSON environment variable: {e}. Falling back to credentials.json."
            )
    # Fallback to credentials.json for local development or if env var is missing/invalid
    return service_account.Credentials.from_service_account_file(
        "credentials.json",
        scopes=SCOPES,
    )


def get_drive_service():
    creds = load_credentials()
    return build("drive", "v3", credentials=creds)


def get_sheets_service():
    """Return raw Sheets API client (Google API Resource)"""
    creds = load_credentials()
    return build("sheets", "v4", credentials=creds)


def get_gspread_client():
    """Return gspread client for convenient worksheet editing"""
    creds = load_credentials()
    return gspread.authorize(creds)


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
    log.debug(f"Uploading file '{filepath}' to Drive folder ID '{parent_id}'")
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
    log.info(f"📄 Uploaded to Drive as Google Sheet: {filepath}")
    log.debug(f"Uploaded file ID: {uploaded['id']}")

    # After uploading, use gspread to open the sheet and check for 'sep=' in first row of all worksheets
    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(uploaded["id"])
    for sheet in spreadsheet.worksheets():
        first_row = sheet.row_values(1)
        if first_row and first_row[0].strip().lower().startswith("sep="):
            sheet.delete_rows(1)

    return uploaded["id"]


def list_files_in_drive_folder(drive, folder_id):
    log.debug(f"Listing files in Drive folder ID: {folder_id}")
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
    log.debug(f"Found {len(files)} files in folder ID: {folder_id}")
    return files


def download_file(drive, file_id, destination_path):
    log.debug(f"Downloading file ID '{file_id}' to '{destination_path}'")
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(destination_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()
    log.info(f"⬇️ Downloaded file to {destination_path}")


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
    log.debug(f"Applying formatting to spreadsheet ID: {spreadsheet_id}")
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(spreadsheet_id)
        sheet = sh.sheet1
        log.debug("Opened sheet for formatting")

        # Get all values to determine range size
        values = sheet.get_all_values()
        if not values or len(values) == 0 or len(values[0]) == 0:
            log.warning("Sheet is empty, skipping formatting")
            return

        apply_sheet_formatting(sheet)

        log.info("✅ Formatting applied successfully")
    except Exception as e:
        log.error(f"Error applying formatting: {e}")


def get_or_create_subfolder(drive_service, parent_folder_id, subfolder_name):
    """
    Gets or creates a subfolder inside a shared drive or My Drive.
    Returns the folder ID.
    """
    query = (
        f"mimeType='application/vnd.google-apps.folder' and "
        f"name='{subfolder_name}' and "
        f"'{parent_folder_id}' in parents and trashed=false"
    )
    response = (
        drive_service.files()
        .list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )

    files = response.get("files", [])
    if files:
        return files[0]["id"]

    file_metadata = {
        "name": subfolder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_folder_id],
    }
    folder = (
        drive_service.files()
        .create(body=file_metadata, fields="id", supportsAllDrives=True)
        .execute()
    )

    return folder.get("id")


def get_file_by_name(drive_service, folder_id, filename):
    """
    Returns the file metadata for a file with a given name in a folder, or None if not found.
    """
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    response = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = response.get("files", [])
    if files:
        return files[0]
    return None


def create_spreadsheet(
    drive_service,
    name,
    parent_folder_id,
    mime_type: str = "application/vnd.google-apps.spreadsheet",
):
    """
    Finds a file by name in the specified folder. If not found, creates a new file with that name.
    This function supports Shared Drives (supportsAllDrives=True).
    Returns the file ID.
    """
    log.info(
        f"🔍 Searching for file '{name}' in folder ID {parent_folder_id} (shared drives enabled)"
    )
    try:
        query = f"'{parent_folder_id}' in parents and name = '{name}' and mimeType = '{mime_type}' and trashed = false"
        response = (
            drive_service.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files = response.get("files", [])
        if files:
            log.info(f"📄 Found existing file '{name}' with ID {files[0]['id']}")
            return files[0]["id"]
        else:
            log.info(
                f"➕ No existing file named '{name}' — creating new one in parent {parent_folder_id}"
            )
            file_metadata = {"name": name, "mimeType": mime_type, "parents": [parent_folder_id]}
            file = (
                drive_service.files()
                .create(body=file_metadata, fields="id", supportsAllDrives=True)
                .execute()
            )
            log.info(f"🆕 Created new file '{name}' with ID {file['id']}")
            return file["id"]
    except HttpError as error:
        log.error(f"An error occurred while finding or creating file: {error}")
        raise


def move_file_to_folder(drive_service, file_id, folder_id):
    """
    Moves a file to a specified folder.
    """
    # Get current parents
    file = drive_service.files().get(fileId=file_id, fields="parents").execute()
    previous_parents = ",".join(file.get("parents", []))
    # Move the file to the new folder
    drive_service.files().update(
        fileId=file_id, addParents=folder_id, removeParents=previous_parents, fields="id, parents"
    ).execute()


def remove_file_from_root(drive_service, file_id):
    """
    Removes a file from the root folder.
    """
    file = drive_service.files().get(fileId=file_id, fields="parents").execute()
    parents = file.get("parents", [])
    if "root" in parents:
        drive_service.files().update(
            fileId=file_id, removeParents="root", fields="id, parents"
        ).execute()


def delete_all_sheets_except(sheets_service, spreadsheet_id, sheet_to_keep):
    """
    Deletes all sheets except the one named sheet_to_keep.
    """
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet.get("sheets", [])
    requests = []
    for sheet in sheets:
        title = sheet["properties"]["title"]
        sheet_id = sheet["properties"]["sheetId"]
        if title != sheet_to_keep:
            requests.append({"deleteSheet": {"sheetId": sheet_id}})
    if requests:
        body = {"requests": requests}
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body=body
        ).execute()


def set_values(sheets_service, spreadsheet_id, sheet_name, start_row, start_col, values):
    """
    Sets values in a sheet starting at (start_row, start_col).
    """
    end_row = start_row + len(values) - 1
    end_col = start_col + len(values[0]) - 1 if values else start_col
    range_name = f"{sheet_name}!R{start_row}C{start_col}:R{end_row}C{end_col}"
    body = {"values": values}
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name, valueInputOption="RAW", body=body
    ).execute()


def set_bold_font(
    sheets_service, spreadsheet_id, sheet_id, start_row, end_row, start_col, end_col
):
    """
    Sets font weight to bold for the specified range.
    """
    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row - 1,
                    "endRowIndex": end_row,
                    "startColumnIndex": start_col - 1,
                    "endColumnIndex": end_col,
                },
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        }
    ]
    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def freeze_rows(sheets_service, spreadsheet_id, sheet_id, num_rows):
    """
    Freezes the specified number of rows at the top of the sheet.
    """
    requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": num_rows},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        }
    ]
    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def set_horizontal_alignment(
    sheets_service,
    spreadsheet_id,
    sheet_id,
    start_row,
    end_row,
    start_col,
    end_col,
    alignment="LEFT",
):
    """
    Sets horizontal alignment for a range.
    """
    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row - 1,
                    "endRowIndex": end_row,
                    "startColumnIndex": start_col - 1,
                    "endColumnIndex": end_col,
                },
                "cell": {"userEnteredFormat": {"horizontalAlignment": alignment}},
                "fields": "userEnteredFormat.horizontalAlignment",
            }
        }
    ]
    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def set_number_format(
    sheets_service, spreadsheet_id, sheet_id, start_row, end_row, start_col, end_col, format_str
):
    """
    Sets number format for a range.
    """
    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row - 1,
                    "endRowIndex": end_row,
                    "startColumnIndex": start_col - 1,
                    "endColumnIndex": end_col,
                },
                "cell": {
                    "userEnteredFormat": {"numberFormat": {"type": "TEXT", "pattern": format_str}}
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        }
    ]
    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def auto_resize_columns(sheets_service, spreadsheet_id, sheet_id, start_col, end_col):
    """
    Auto-resizes columns from start_col to end_col.
    """
    requests = [
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": start_col - 1,
                    "endIndex": end_col,
                }
            }
        }
    ]
    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def update_sheet_values(sheets_service, spreadsheet_id, sheet_name, values):
    """
    Update values in the sheet starting from A1.
    """
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=sheet_name,
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()


def set_sheet_formatting(
    spreadsheet_id, sheet_id, header_row_count, total_rows, total_cols, backgrounds
):
    """
    Apply formatting to a Google Sheet:
    - Freeze header rows
    - Set font bold for header row
    - Set horizontal alignment left for all data
    - Set number format to plain text for data rows
    - Set background colors for data rows
    - Auto resize columns
    """
    sheets_service = get_sheets_service()
    requests = []

    # Freeze header rows
    requests.append(
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": header_row_count},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        }
    )

    # Bold font for header row
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": header_row_count,
                    "startColumnIndex": 0,
                    "endColumnIndex": total_cols,
                },
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        }
    )

    # Horizontal alignment left for all data
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": total_rows,
                    "startColumnIndex": 0,
                    "endColumnIndex": total_cols,
                },
                "cell": {"userEnteredFormat": {"horizontalAlignment": "LEFT"}},
                "fields": "userEnteredFormat.horizontalAlignment",
            }
        }
    )

    # Number format plain text for data rows (excluding header)
    if total_rows > header_row_count:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": header_row_count,
                        "endRowIndex": total_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": total_cols,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {"type": "TEXT", "pattern": "@STRING@"}
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )

    # Set background colors for data rows (excluding header)
    if len(backgrounds) > 1:
        bg_requests = []
        for row_idx, bg_colors in enumerate(backgrounds[1:], start=header_row_count):
            row_request = {
                "updateCells": {
                    "rows": [
                        {
                            "values": [
                                {
                                    "userEnteredFormat": {
                                        "backgroundColor": helpers.hex_to_rgb(color)
                                    }
                                }
                                for color in bg_colors
                            ]
                        }
                    ],
                    "fields": "userEnteredFormat.backgroundColor",
                    "start": {"sheetId": sheet_id, "rowIndex": row_idx, "columnIndex": 0},
                }
            }
            bg_requests.append(row_request)
        requests.extend(bg_requests)

    # Auto resize columns
    for col in range(total_cols):
        requests.append(
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": col,
                        "endIndex": col + 1,
                    }
                }
            }
        )
    # Can't set max width directly via API; auto-resize only.

    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def clear_sheet(sheets_service, spreadsheet_id, sheet_name):
    # Get sheetId from sheet name
    metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    for sheet in metadata["sheets"]:
        if sheet["properties"]["title"] == sheet_name:
            sheet_id = sheet["properties"]["sheetId"]
            break

    if sheet_id is None:
        raise ValueError(f"Sheet name '{sheet_name}' not found in spreadsheet.")

    body = {"requests": [{"updateCells": {"range": {"sheetId": sheet_id}, "fields": "*"}}]}

    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def list_files_in_folder(drive_service, folder_id):
    """
    List all files in a folder (non-recursive).
    Returns a list of dicts with 'id', 'name', and 'mimeType'.
    """
    files = []
    page_token = None
    while True:
        response = (
            drive_service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed=false",
                spaces="drive",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
            )
            .execute()
        )
        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken", None)
        if page_token is None:
            break
    return files


def get_sheet_values(sheets_service, spreadsheet_id, sheet_name):
    """
    Get all values from the given sheet.
    Returns a list of rows (each row is a list of strings).
    """
    range_name = f"{sheet_name}"
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name, majorDimension="ROWS")
        .execute()
    )
    values = result.get("values", [])
    # Normalize all values to strings
    normalized = []
    for row in values:
        normalized.append([str(cell) if cell is not None else "" for cell in row])
    return normalized


def clear_all_except_one_sheet(sheets_service, spreadsheet_id: str, sheet_to_keep: str):
    """
    Deletes all sheets in the spreadsheet except the one specified.
    If the sheet_to_keep does not exist, creates it.
    """
    log.info(f"🧹 Clearing all sheets except '{sheet_to_keep}' in spreadsheet ID {spreadsheet_id}")
    try:
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get("sheets", [])
        sheet_titles = [sheet["properties"]["title"] for sheet in sheets]
        requests = []
        # Create the sheet_to_keep if it does not exist
        if sheet_to_keep not in sheet_titles:
            log.info(f"➕ Sheet '{sheet_to_keep}' not found, queuing create request")
            requests.append({"addSheet": {"properties": {"title": sheet_to_keep}}})
        # Delete all sheets except sheet_to_keep
        for sheet in sheets:
            title = sheet["properties"]["title"]
            sheet_id = sheet["properties"]["sheetId"]
            if title != sheet_to_keep:
                log.info(f"❌ Queuing deletion of sheet '{title}' (id {sheet_id})")
                requests.append({"deleteSheet": {"sheetId": sheet_id}})
        if requests:
            body = {"requests": requests}
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body
            ).execute()
            log.info("✅ Sheets updated successfully (clear/create/delete performed)")
        else:
            log.info("ℹ️ No sheet changes required")
    except HttpError as error:
        log.error(f"An error occurred while clearing sheets: {error}")
        raise


def get_all_subfolders(drive_service, parent_folder_id: str) -> List[Dict]:
    """
    Returns a list of all subfolders in the specified parent folder.
    Supports Shared Drives.
    """
    log.info(
        f"📂 Retrieving all subfolders in folder ID {parent_folder_id} (shared drives enabled)"
    )
    try:
        query = f"'{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        folders = []
        page_token = None
        while True:
            response = (
                drive_service.files()
                .list(
                    q=query,
                    spaces="drive",
                    fields="nextPageToken, files(id, name)",
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )
            folders.extend(response.get("files", []))
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break
        log.info(f"📂 Found {len(folders)} subfolders under {parent_folder_id}")
        return folders
    except HttpError as error:
        log.error(f"An error occurred while retrieving subfolders: {error}")
        raise


def get_files_in_folder(service, folder_id, name_contains=None, mime_type=None, trashed=False):
    """Returns a list of files in a Google Drive folder, optionally filtering by name substring and MIME type."""
    query = f"'{folder_id}' in parents"
    if name_contains:
        query += f" and name contains '{name_contains}'"
    if mime_type:
        query += f" and mimeType = '{mime_type}'"
    if trashed is False:
        query += " and trashed = false"

    results = (
        service.files()
        .list(
            q=query,
            spaces="drive",
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )

    return results.get("files", [])


def insert_rows(sheets_service, spreadsheet_id: str, sheet_name: str, values: List[List]):
    """
    Inserts rows into the specified sheet (overwrites the range starting at A1).
    Uses USER_ENTERED so formulas like HYPERLINK() are written as formulas.
    """
    log.info(
        f"➕ Inserting {len(values)} rows into sheet '{sheet_name}' in spreadsheet {spreadsheet_id}"
    )
    try:
        range_ = f"{sheet_name}!A1"
        body = {"values": values}
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=range_, valueInputOption="USER_ENTERED", body=body
        ).execute()
        log.info("✅ Rows inserted successfully")
    except HttpError as error:
        log.error(f"An error occurred while inserting rows: {error}")
        raise


def set_column_formatting(sheets_service, spreadsheet_id: str, sheet_name: str, num_columns: int):
    """
    Sets formatting for specified columns (first column date, others text).
    """
    log.info(f"🎨 Setting column formatting for {num_columns} columns in sheet '{sheet_name}'")
    try:
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = None
        for sheet in spreadsheet.get("sheets", []):
            if sheet["properties"]["title"] == sheet_name:
                sheet_id = sheet["properties"]["sheetId"]
                break
        if sheet_id is None:
            log.warning(f"Sheet '{sheet_name}' not found for formatting")
            return

        requests = []
        # Format first column as DATE
        if num_columns >= 1:
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 1000000,
                            "startColumnIndex": 0,
                            "endColumnIndex": 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}
                            }
                        },
                        "fields": "userEnteredFormat.numberFormat",
                    }
                }
            )
        # Format other columns as TEXT
        if num_columns > 1:
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 1000000,
                            "startColumnIndex": 1,
                            "endColumnIndex": num_columns,
                        },
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "TEXT"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                }
            )

        if requests:
            body = {"requests": requests}
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body
            ).execute()
            log.info("✅ Column formatting set successfully")
    except HttpError as error:
        log.error(f"An error occurred while setting column formatting: {error}")
        raise


def delete_sheet_by_name(sheets_service, spreadsheet_id: str, sheet_name: str):
    """
    Deletes a sheet by its name from the spreadsheet.
    """
    log.info(f"🗑️ Deleting sheet '{sheet_name}' if it exists")
    try:
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get("sheets", [])
        if len(sheets) <= 1:
            log.warning(f"Not deleting sheet '{sheet_name}': spreadsheet only has one sheet.")
            return
        sheet_id = None
        for sheet in sheets:
            if sheet["properties"]["title"] == sheet_name:
                sheet_id = sheet["properties"]["sheetId"]
                break
        if sheet_id is not None:
            body = {"requests": [{"deleteSheet": {"sheetId": sheet_id}}]}
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body
            ).execute()
            log.info(f"✅ Sheet '{sheet_name}' deleted successfully")
        else:
            log.info(f"Sheet '{sheet_name}' not found; no deletion necessary")
    except HttpError as error:
        log.error(f"An error occurred while deleting sheet: {error}")
        raise


def get_spreadsheet_metadata(sheets_service, spreadsheet_id: str) -> Dict:
    """
    Retrieves the metadata of the spreadsheet, including sheets info.
    """
    log.info(f"🔍 Retrieving spreadsheet metadata for ID {spreadsheet_id}")
    try:
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        return spreadsheet
    except HttpError as error:
        log.error(f"An error occurred while retrieving spreadsheet metadata: {error}")
        raise


def reorder_sheets(
    sheets_service,
    spreadsheet_id: str,
    sheet_names_in_order: List[str],
    spreadsheet_metadata: Dict,
):
    """
    Reorders sheets in the spreadsheet to match the order of sheet_names_in_order.
    Sheets not in the list will be placed after those specified.
    """
    log.info(
        f"🔀 Reordering sheets in spreadsheet ID {spreadsheet_id} to order: {sheet_names_in_order}"
    )
    try:
        sheets = spreadsheet_metadata.get("sheets", [])
        title_to_id = {
            sheet["properties"]["title"]: sheet["properties"]["sheetId"] for sheet in sheets
        }
        requests = []
        index = 0
        for name in sheet_names_in_order:
            sheet_id = title_to_id.get(name)
            if sheet_id is not None:
                requests.append(
                    {
                        "updateSheetProperties": {
                            "properties": {"sheetId": sheet_id, "index": index},
                            "fields": "index",
                        }
                    }
                )
                index += 1
        remaining_sheets = [
            sheet for sheet in sheets if sheet["properties"]["title"] not in sheet_names_in_order
        ]
        for sheet in remaining_sheets:
            requests.append(
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": sheet["properties"]["sheetId"], "index": index},
                        "fields": "index",
                    }
                }
            )
            index += 1
        if requests:
            body = {"requests": requests}
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body
            ).execute()
            log.info("✅ Sheets reordered successfully")
    except HttpError as error:
        log.error(f"An error occurred while reordering sheets: {error}")
        raise


def find_or_create_file_by_name(
    drive_service,
    name: str,
    parent_folder_id: str,
    mime_type: str = "application/vnd.google-apps.spreadsheet",
) -> str:
    """
    Finds a file by name in the specified folder. If not found, creates a new file with that name.
    This function supports Shared Drives (supportsAllDrives=True).
    Returns the file ID.
    """
    log.info(
        f"🔍 Searching for file '{name}' in folder ID {parent_folder_id} (shared drives enabled)"
    )
    try:
        query = f"'{parent_folder_id}' in parents and name = '{name}' and mimeType = '{mime_type}' and trashed = false"
        response = (
            drive_service.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files = response.get("files", [])
        if files:
            log.info(f"📄 Found existing file '{name}' with ID {files[0]['id']}")
            return files[0]["id"]
        else:
            log.info(
                f"➕ No existing file named '{name}' — creating new one in parent {parent_folder_id}"
            )
            file_metadata = {"name": name, "mimeType": mime_type, "parents": [parent_folder_id]}
            file = (
                drive_service.files()
                .create(body=file_metadata, fields="id", supportsAllDrives=True)
                .execute()
            )
            log.info(f"🆕 Created new file '{name}' with ID {file['id']}")
            return file["id"]
    except HttpError as error:
        log.error(f"An error occurred while finding or creating file: {error}")
        raise


def get_sheet_titles(sheets_service, spreadsheet_id: str) -> list[str]:
    sheets = (
        sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute().get("sheets", [])
    )
    return [sheet["properties"]["title"] for sheet in sheets]


def ensure_sheet_exists(sheet_service, spreadsheet_id: str, sheet_name: str) -> None:
    """
    Ensures that a sheet with the given name exists in the spreadsheet. If it does not exist, it is created.
    """
    metadata = sheet_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_names = [s["properties"]["title"] for s in metadata.get("sheets", [])]
    if sheet_name not in sheet_names:
        body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        sheet_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def write_sheet_data(
    sheet_service, spreadsheet_id: str, sheet_name: str, header: List[str], rows: List[List[Any]]
) -> None:
    """
    Overwrites the specified sheet in the given spreadsheet with the provided header and rows.

    If the sheet does not exist, it will be created.
    If the sheet exists, its contents will be cleared before writing.

    Args:
        sheet_service: The Google Sheets API service instance.
        spreadsheet_id (str): The ID of the spreadsheet.
        sheet_name (str): The name of the sheet to write data to.
        header (List[str]): A list of column headers.
        rows (List[List[Any]]): A list of data rows (each a list of cell values).
    """
    # Ensure the sheet exists or create it
    ensure_sheet_exists(sheet_service, spreadsheet_id, sheet_name)

    # Clear existing data
    clear_range = f"{sheet_name}!A:Z"
    sheet_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id, range=clear_range, body={}
    ).execute()

    # Prepare values for update
    values = [header] + rows
    body = {"values": values}

    # Write new data
    sheet_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A1", valueInputOption="RAW", body=body
    ).execute()


def format_summary_sheet(
    sheet_service, spreadsheet_id: str, sheet_name: str, header: List[str], rows: List[List[Any]]
) -> None:
    """
    Applies formatting to a summary sheet, such as:
    - Bold header row
    - Frozen header
    - Auto-sized columns (limited width)
    - Plain text formatting
    - Gridlines and optional visual enhancements

    Args:
        sheet_service: The Google Sheets API service instance.
        spreadsheet_id (str): The ID of the spreadsheet.
        sheet_name (str): The sheet name to format.
        header (List[str]): The list of column headers (used for column count).
        rows (List[List[Any]]): The data rows (used for row count).
    """
    sheet_id = get_sheet_id_by_name(sheet_service, spreadsheet_id, sheet_name)
    requests = []

    num_columns = len(header)
    num_rows = len(rows) + 1  # +1 for header

    # Freeze header row
    requests.append(
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        }
    )

    # Bold the header row
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                },
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        }
    )

    # Auto resize columns (limited width)
    requests.append(
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": num_columns,
                }
            }
        }
    )

    # Format all cells as plain text
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": num_rows,
                    "startColumnIndex": 0,
                    "endColumnIndex": num_columns,
                },
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "TEXT"}}},
                "fields": "userEnteredFormat.numberFormat",
            }
        }
    )

    # Send all formatting requests in batch
    sheet_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": requests}
    ).execute()


def get_sheet_id_by_name(sheet_service, spreadsheet_id: str, sheet_name: str) -> int:
    """
    Returns the numeric sheet ID of the given sheet name.
    """
    metadata = sheet_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in metadata.get("sheets", []):
        if sheet.get("properties", {}).get("title") == sheet_name:
            return sheet.get("properties", {}).get("sheetId")
    raise ValueError(f"Sheet '{sheet_name}' not found in spreadsheet.")


def rename_sheet(sheets_service, spreadsheet_id, sheet_id, new_title):
    """
    Renames a sheet within a spreadsheet.
    """
    body = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": sheet_id, "title": new_title},
                    "fields": "title",
                }
            }
        ]
    }
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def extract_date_from_filename(filename):
    import re

    match = re.match(r"(\d{4}-\d{2}-\d{2})", filename)
    return match.group(1) if match else filename


def parse_m3u(sheets_service, filepath, spreadsheet_id):
    """Parses .m3u file and returns a list of (artist, title, extvdj_line) tuples."""
    import re

    songs = []
    sheets_service.log_debug(spreadsheet_id, f"Opening M3U file: {filepath}")
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()
        sheets_service.log_debug(spreadsheet_id, f"Read {len(lines)} lines from {filepath}")
        for line in lines:
            line = line.strip()
            # sheets.log_debug(spreadsheet_id, f"Stripped line: {line}")
            if line.startswith("#EXTVDJ:"):
                artist_match = re.search(r"<artist>(.*?)</artist>", line)
                title_match = re.search(r"<title>(.*?)</title>", line)
                if artist_match and title_match:
                    artist = artist_match.group(1).strip()
                    title = title_match.group(1).strip()
                    songs.append((artist, title, line))
            #        sheets.log_debug(spreadsheet_id, f"Parsed song - Artist: '{artist}', Title: '{title}'")
            #    else:
            #        sheets.log_debug(spreadsheet_id, f"Missing artist or title in line: {line}")
            # else:
            #    sheets.log_debug(spreadsheet_id, f"Ignored line: {line}")
    sheets_service.log_debug(spreadsheet_id, f"Total parsed songs: {len(songs)}")
    return songs


def find_file_by_name(drive_service, folder_id, target_name):
    files = drive_service.list_files_in_folder(folder_id)
    for f in files:
        if f["name"] == target_name:
            return f
    raise FileNotFoundError(f"File named {target_name} not found in folder {folder_id}")
