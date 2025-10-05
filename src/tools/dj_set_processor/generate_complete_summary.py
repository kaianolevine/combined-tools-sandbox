import logging
import re
from collections import OrderedDict

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants (replace with your actual folder ID and scopes)
DJ_SETS = "1A0tKQ2DBXI1Bt9h--olFwnBNne3am-rL"
SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = "credentials.json"


def authenticate():
    """
    Authenticates and returns the Google Drive and Sheets service clients.
    """
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    return drive_service, sheets_service


def get_or_create_subfolder(drive_service, parent_folder_id, subfolder_name):
    """
    Retrieves a subfolder by name under the parent folder. Creates it if it does not exist.
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
        folder_id = files[0]["id"]
        logger.info(f'Found existing folder "{subfolder_name}" with ID: {folder_id}')
        return folder_id
    else:
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
        folder_id = folder.get("id")
        logger.info(f'Created folder "{subfolder_name}" with ID: {folder_id}')
        return folder_id


def list_files_in_folder(drive_service, folder_id):
    """
    Lists all files in a folder.
    """
    files = []
    page_token = None
    query = f"'{folder_id}' in parents and trashed=false"
    while True:
        response = (
            drive_service.files()
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
    return files


def get_file_by_name(drive_service, folder_id, filename):
    """
    Returns the file metadata for a file with a given name in a folder, or None if not found.
    """
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
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
        return files[0]
    return None


def create_spreadsheet(sheets_service, title):
    """
    Creates a new Google Sheets spreadsheet with the given title.
    Returns the spreadsheet ID.
    """
    spreadsheet_body = {"properties": {"title": title}}
    spreadsheet = (
        sheets_service.spreadsheets()
        .create(body=spreadsheet_body, fields="spreadsheetId")
        .execute()
    )
    spreadsheet_id = spreadsheet.get("spreadsheetId")
    logger.info(f'Created spreadsheet "{title}" with ID: {spreadsheet_id}')
    return spreadsheet_id


def move_file_to_folder(drive_service, file_id, folder_id):
    """
    Moves a file to a specified folder.
    """
    # Get current parents
    file = (
        drive_service.files()
        .get(fileId=file_id, fields="parents", supportsAllDrives=True)
        .execute()
    )
    previous_parents = ",".join(file.get("parents", []))
    # Move the file to the new folder
    drive_service.files().update(
        fileId=file_id,
        addParents=folder_id,
        removeParents=previous_parents,
        fields="id, parents",
        supportsAllDrives=True,
    ).execute()


def remove_file_from_root(drive_service, file_id):
    """
    Removes a file from the root folder.
    """
    file = (
        drive_service.files()
        .get(fileId=file_id, fields="parents", supportsAllDrives=True)
        .execute()
    )
    parents = file.get("parents", [])
    if "root" in parents:
        drive_service.files().update(
            fileId=file_id, removeParents="root", fields="id, parents", supportsAllDrives=True
        ).execute()


def get_sheet_values(sheets_service, spreadsheet_id, sheet_name):
    """
    Retrieves all values from a sheet.
    """
    range_name = f"{sheet_name}"
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    values = result.get("values", [])
    return values


def clear_sheet(sheets_service, spreadsheet_id, sheet_name):
    """
    Clears all contents in a sheet.
    """
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id, range=sheet_name
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


def generate_complete_summary():
    """
    Generates a consolidated 'Complete Summary' spreadsheet from yearly summary spreadsheets.
    """
    try:
        drive_service, sheets_service = authenticate()
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        return

    # Step 0: Get or create 'Summary' folder inside parent folder DJ_SETS
    try:
        summary_folder_id = get_or_create_subfolder(drive_service, DJ_SETS, "Summary")
    except HttpError as e:
        logger.error(f"Failed to get or create 'Summary' folder: {e}")
        return

    # List all files in summary folder
    try:
        summary_files = list_files_in_folder(drive_service, summary_folder_id)
    except HttpError as e:
        logger.error(f"Failed to list files in Summary folder: {e}")
        return

    # Determine output spreadsheet name
    output_name = "_pending_Complete Summary"
    # Check if any file with name containing "complete summary" exists (case insensitive)
    for f in summary_files:
        if (
            f["mimeType"] == "application/vnd.google-apps.spreadsheet"
            and "complete summary" in f["name"].lower()
        ):
            logger.warning(f'⚠️ Existing "Complete Summary" detected — using name: {output_name}')
            break

    # Check if output file already exists
    existing_file = get_file_by_name(drive_service, summary_folder_id, output_name)
    if existing_file:
        master_file_id = existing_file["id"]
        logger.info(f'Using existing master file "{output_name}" with ID: {master_file_id}')
    else:
        # Create new spreadsheet
        master_file_id = create_spreadsheet(sheets_service, output_name)
        # Move to summary folder
        move_file_to_folder(drive_service, master_file_id, summary_folder_id)
        # Remove from root folder
        remove_file_from_root(drive_service, master_file_id)

    # Open master spreadsheet info
    try:
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=master_file_id).execute()
    except HttpError as e:
        logger.error(f"Failed to open master spreadsheet: {e}")
        return

    # Delete all sheets except "Sheet1"
    delete_all_sheets_except(sheets_service, master_file_id, "Sheet1")
    # Clear "Sheet1"
    clear_sheet(sheets_service, master_file_id, "Sheet1")

    # Step 1: Gather and normalize all rows from summary files
    summary_data = []
    year_set = set()
    # We only want files named like "YYYY Summary"
    year_summary_pattern = re.compile(r"^(\d{4}) Summary$")

    for file in summary_files:
        file_name = file["name"].strip()
        match = year_summary_pattern.match(file_name)
        if not match:
            continue
        year = match.group(1)
        year_set.add(year)
        file_id = file["id"]

        try:
            # Use helper to get first sheet name
            file_spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=file_id).execute()
            sheets_list = file_spreadsheet.get("sheets", [])
            if not sheets_list:
                logger.warning(f'File "{file_name}" has no sheets, skipping.')
                continue
            source_sheet_title = sheets_list[0]["properties"]["title"]
            data = get_sheet_values(sheets_service, file_id, source_sheet_title)
            if len(data) < 2:
                continue
            # Normalize header row
            # headers = normalize_header_row(data[0])
            # lower_headers = [h.lower() for h in headers]
            try:
                pass  # count_index = lower_headers.index("count")
            except ValueError:
                count_index = -1
            rows = data[1:]
            for row in rows:
                count = 1
                if count_index >= 0 and count_index < len(row):
                    try:
                        count = int(row[count_index])
                    except (ValueError, TypeError):
                        count = 1
                # summary_data.append({"year": year, "headers": headers, "row": row, "count": count})
        except HttpError as e:
            logger.error(f'❌ Failed to read "{file_name}": {e}')

    if not summary_data:
        logger.warning("⚠️ No summary files found. Created empty Complete Summary.")
        return

    # Step 2: Build a unified header set using lowercase deduplication
    header_map = OrderedDict()  # lowercased => original casing
    for entry in summary_data:
        for h in entry["headers"]:
            key = h.lower()
            if key != "count" and key not in header_map:
                header_map[key] = h
    base_headers = list(header_map.values())
    years = sorted(year_set)
    final_headers = base_headers + years

    # Step 3: Consolidate rows using stringified deduplication keys
    row_map = dict()
    for entry in summary_data:
        year = entry["year"]
        headers = entry["headers"]
        row = entry["row"]
        count = entry["count"]
        normalized_row = {}
        for i, h in enumerate(headers):
            key = h.lower()
            if key != "count" and key in header_map:
                normalized_row[header_map[key]] = str(row[i]) if i < len(row) else ""
        signature = "|".join(normalized_row.get(h, "") for h in base_headers)
        if signature not in row_map:
            year_data = {y: "" for y in years}
            row_map[signature] = {**normalized_row, **year_data}
        existing_count = row_map[signature].get(year, "")
        try:
            existing_count_int = int(existing_count) if existing_count else 0
        except ValueError:
            existing_count_int = 0
        row_map[signature][year] = str(existing_count_int + count)

    # Step 4: Write to sheet
    final_rows = [final_headers]
    for row_obj in row_map.values():
        final_rows.append([row_obj.get(h, "") for h in final_headers])
    set_values(sheets_service, master_file_id, "Sheet1", 1, 1, final_rows)

    # Get sheet ID of "Sheet1"
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=master_file_id).execute()
    sheet_id = None
    for sheet in spreadsheet.get("sheets", []):
        if sheet["properties"]["title"] == "Sheet1":
            sheet_id = sheet["properties"]["sheetId"]
            break
    if sheet_id is None:
        logger.error('Sheet "Sheet1" not found in master spreadsheet.')
        return

    # Apply formatting using shared utilities
    set_bold_font(sheets_service, master_file_id, sheet_id, 1, 1, 1, len(final_headers))
    freeze_rows(sheets_service, master_file_id, sheet_id, 1)
    set_horizontal_alignment(
        sheets_service,
        master_file_id,
        sheet_id,
        1,
        len(final_rows),
        1,
        len(final_headers),  # ,
        # DEFAULT_ALIGNMENT,
    )
    if len(final_rows) > 1:
        set_number_format(
            sheets_service,
            master_file_id,
            sheet_id,
            2,
            len(final_rows),
            1,
            len(final_headers),  # ,
            # PLAIN_TEXT_NUMBER_FORMAT,
        )
    auto_resize_columns(sheets_service, master_file_id, sheet_id, 1, len(final_headers))
    logger.info(f"✅ {output_name} generated.")


if __name__ == "__main__":
    generate_complete_summary()
