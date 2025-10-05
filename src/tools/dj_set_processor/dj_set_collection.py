import re
from typing import List, Dict, Tuple
import tools.dj_set_processor.google_api as google_api
import tools.dj_set_processor.config as config
from googleapiclient.errors import HttpError


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
    config.logger.info(
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
            config.logger.info(f"📄 Found existing file '{name}' with ID {files[0]['id']}")
            return files[0]["id"]
        else:
            config.logger.info(
                f"➕ No existing file named '{name}' — creating new one in parent {parent_folder_id}"
            )
            file_metadata = {"name": name, "mimeType": mime_type, "parents": [parent_folder_id]}
            file = (
                drive_service.files()
                .create(body=file_metadata, fields="id", supportsAllDrives=True)
                .execute()
            )
            config.logger.info(f"🆕 Created new file '{name}' with ID {file['id']}")
            return file["id"]
    except HttpError as error:
        config.logger.error(f"An error occurred while finding or creating file: {error}")
        raise


def clear_all_except_one_sheet(sheets_service, spreadsheet_id: str, sheet_to_keep: str):
    """
    Deletes all sheets in the spreadsheet except the one specified.
    If the sheet_to_keep does not exist, creates it.
    """
    config.logger.info(
        f"🧹 Clearing all sheets except '{sheet_to_keep}' in spreadsheet ID {spreadsheet_id}"
    )
    try:
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get("sheets", [])
        sheet_titles = [sheet["properties"]["title"] for sheet in sheets]
        requests = []
        # Create the sheet_to_keep if it does not exist
        if sheet_to_keep not in sheet_titles:
            config.logger.info(f"➕ Sheet '{sheet_to_keep}' not found, queuing create request")
            requests.append({"addSheet": {"properties": {"title": sheet_to_keep}}})
        # Delete all sheets except sheet_to_keep
        for sheet in sheets:
            title = sheet["properties"]["title"]
            sheet_id = sheet["properties"]["sheetId"]
            if title != sheet_to_keep:
                config.logger.info(f"❌ Queuing deletion of sheet '{title}' (id {sheet_id})")
                requests.append({"deleteSheet": {"sheetId": sheet_id}})
        if requests:
            body = {"requests": requests}
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body
            ).execute()
            config.logger.info("✅ Sheets updated successfully (clear/create/delete performed)")
        else:
            config.logger.info("ℹ️ No sheet changes required")
    except HttpError as error:
        config.logger.error(f"An error occurred while clearing sheets: {error}")
        raise


def get_all_subfolders(drive_service, parent_folder_id: str) -> List[Dict]:
    """
    Returns a list of all subfolders in the specified parent folder.
    Supports Shared Drives.
    """
    config.logger.info(
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
        config.logger.info(f"📂 Found {len(folders)} subfolders under {parent_folder_id}")
        return folders
    except HttpError as error:
        config.logger.error(f"An error occurred while retrieving subfolders: {error}")
        raise


def get_files_in_folder(drive_service, folder_id: str) -> List[Dict]:
    """
    Returns a list of files in the specified folder.
    Supports Shared Drives.
    """
    config.logger.info(f"📄 Retrieving files in folder ID {folder_id} (shared drives enabled)")
    try:
        query = f"'{folder_id}' in parents and trashed = false"
        files = []
        page_token = None
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
        config.logger.info(f"📄 Found {len(files)} files in folder {folder_id}")
        return files
    except HttpError as error:
        config.logger.error(f"An error occurred while retrieving files: {error}")
        raise


def insert_rows(sheets_service, spreadsheet_id: str, sheet_name: str, values: List[List]):
    """
    Inserts rows into the specified sheet (overwrites the range starting at A1).
    Uses USER_ENTERED so formulas like HYPERLINK() are written as formulas.
    """
    config.logger.info(
        f"➕ Inserting {len(values)} rows into sheet '{sheet_name}' in spreadsheet {spreadsheet_id}"
    )
    try:
        range_ = f"{sheet_name}!A1"
        body = {"values": values}
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=range_, valueInputOption="USER_ENTERED", body=body
        ).execute()
        config.logger.info("✅ Rows inserted successfully")
    except HttpError as error:
        config.logger.error(f"An error occurred while inserting rows: {error}")
        raise


def set_column_formatting(sheets_service, spreadsheet_id: str, sheet_name: str, num_columns: int):
    """
    Sets formatting for specified columns (first column date, others text).
    """
    config.logger.info(
        f"🎨 Setting column formatting for {num_columns} columns in sheet '{sheet_name}'"
    )
    try:
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = None
        for sheet in spreadsheet.get("sheets", []):
            if sheet["properties"]["title"] == sheet_name:
                sheet_id = sheet["properties"]["sheetId"]
                break
        if sheet_id is None:
            config.logger.warning(f"Sheet '{sheet_name}' not found for formatting")
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
            config.logger.info("✅ Column formatting set successfully")
    except HttpError as error:
        config.logger.error(f"An error occurred while setting column formatting: {error}")
        raise


def delete_sheet_by_name(sheets_service, spreadsheet_id: str, sheet_name: str):
    """
    Deletes a sheet by its name from the spreadsheet.
    """
    config.logger.info(f"🗑️ Deleting sheet '{sheet_name}' if it exists")
    try:
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get("sheets", [])
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
            config.logger.info(f"✅ Sheet '{sheet_name}' deleted successfully")
        else:
            config.logger.info(f"Sheet '{sheet_name}' not found; no deletion necessary")
    except HttpError as error:
        config.logger.error(f"An error occurred while deleting sheet: {error}")
        raise


def get_spreadsheet_metadata(sheets_service, spreadsheet_id: str) -> Dict:
    """
    Retrieves the metadata of the spreadsheet, including sheets info.
    """
    config.logger.info(f"🔍 Retrieving spreadsheet metadata for ID {spreadsheet_id}")
    try:
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        return spreadsheet
    except HttpError as error:
        config.logger.error(f"An error occurred while retrieving spreadsheet metadata: {error}")
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
    config.logger.info(
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
            config.logger.info("✅ Sheets reordered successfully")
    except HttpError as error:
        config.logger.error(f"An error occurred while reordering sheets: {error}")
        raise


def extract_date_and_title(file_name: str) -> Tuple[str, str]:
    match = re.match(r"^(\d{4}-\d{2}-\d{2})(.*)", file_name)
    if not match:
        return ("", file_name)
    date = match[1]
    title = match[2].lstrip("-_ ")
    return (date, title)


def generate_dj_set_collection():
    config.logger.info("🚀 Starting generate_dj_set_collection")
    drive_service = google_api.get_drive_service()
    sheets_service = google_api.get_sheets_service()

    # Locate DJ_SETS folder (we assume the constant ID points to the shared drive folder or folder in shared drive)
    parent_folder_id = config.DJ_SETS
    config.logger.info(f"📁 Using DJ_SETS folder: {parent_folder_id}")

    # Check for existing file or create new (create directly in the shared drive parent)
    spreadsheet_id = find_or_create_file_by_name(
        drive_service,
        config.OUTPUT_NAME,
        parent_folder_id,
        mime_type="application/vnd.google-apps.spreadsheet",
    )
    config.logger.info(f"📄 Spreadsheet ID: {spreadsheet_id}")

    # Ensure there's exactly one temp sheet to start from
    clear_all_except_one_sheet(sheets_service, spreadsheet_id, config.TEMP_TAB_NAME)

    # Enumerate subfolders in DJ_SETS
    subfolders = get_all_subfolders(drive_service, parent_folder_id)
    subfolders.sort(key=lambda f: f["name"], reverse=True)

    tabs_to_add: List[str] = []

    for folder in subfolders:
        name = folder["name"]
        folder_id = folder["id"]
        config.logger.info(f"📁 Processing folder: {name} (id: {folder_id})")

        files = get_files_in_folder(drive_service, folder_id)
        rows = []

        for f in files:
            if f.get("mimeType") != "application/vnd.google-apps.spreadsheet":
                continue
            file_name = f["name"]
            file_url = f"https://docs.google.com/spreadsheets/d/{f['id']}"

            if name.lower() == "summary":
                year_match = re.match(r"^(\d{4})", file_name)
                year = year_match.group(1) if year_match else ""
                rows.append([year, f'=HYPERLINK("{file_url}", "{file_name}")'])
            else:
                date, title = extract_date_and_title(file_name)
                rows.append([date, title, f'=HYPERLINK("{file_url}", "{file_name}")'])

        if name.lower() == "summary":
            if rows:
                complete = [r for r in rows if not r[0]]
                others = sorted([r for r in rows if r[0]], key=lambda r: r[0], reverse=True)
                all_rows = complete + others
                # add Summary sheet
                config.logger.info("➕ Adding Summary sheet and inserting rows")
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={
                        "requests": [
                            {"addSheet": {"properties": {"title": config.SUMMARY_TAB_NAME}}}
                        ]
                    },
                ).execute()
                insert_rows(
                    sheets_service,
                    spreadsheet_id,
                    config.SUMMARY_TAB_NAME,
                    [["Year", "Link"]] + all_rows,
                )
                set_column_formatting(sheets_service, spreadsheet_id, config.SUMMARY_TAB_NAME, 2)
        elif rows:
            rows.sort(key=lambda r: r[0], reverse=True)
            config.logger.info(
                f"➕ Adding sheet for folder '{name}' and inserting {len(rows)} rows"
            )
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": name}}}]},
            ).execute()
            insert_rows(sheets_service, spreadsheet_id, name, [["Date", "Name", "Link"]] + rows)
            set_column_formatting(sheets_service, spreadsheet_id, name, 3)
            tabs_to_add.append(name)

    # Clean up temp sheets if any
    delete_sheet_by_name(sheets_service, spreadsheet_id, config.TEMP_TAB_NAME)
    delete_sheet_by_name(sheets_service, spreadsheet_id, "Sheet1")

    # Reorder sheets: tabs_to_add then Summary
    metadata = get_spreadsheet_metadata(sheets_service, spreadsheet_id)
    reorder_sheets(
        sheets_service, spreadsheet_id, tabs_to_add + [config.SUMMARY_TAB_NAME], metadata
    )

    config.logger.info("✅ Finished generate_dj_set_collection")


if __name__ == "__main__":
    config.logging.basicConfig(level=config.logging.INFO)
    generate_dj_set_collection()
