from core import _google_credentials
from core import logger as log
from typing import Any, List, Dict
from googleapiclient.errors import HttpError

log = log.get_logger()


def get_sheets_service():
    return _google_credentials.get_sheets_client()


def get_gspread_client():
    return _google_credentials.get_gspread_client()


def get_or_create_sheet(spreadsheet_id: str, sheet_name: str) -> None:
    log.debug(
        f"get_or_create_sheet called with spreadsheet_id={spreadsheet_id}, sheet_name={sheet_name}"
    )
    service = get_sheets_service()
    sheets_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_titles = [s["properties"]["title"] for s in sheets_metadata.get("sheets", [])]
    log.debug(f"Existing sheet titles: {sheet_titles}")
    if sheet_name not in sheet_titles:
        log.info(f"Creating new sheet tab: {sheet_name}")
        add_sheet_body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body=add_sheet_body
        ).execute()
        log.info(f"Sheet '{sheet_name}' created successfully.")
    else:
        log.debug(f"Sheet '{sheet_name}' already exists; no creation needed.")


def read_sheet(spreadsheet_id, range_name):
    log.debug(f"read_sheet called with spreadsheet_id={spreadsheet_id}, range_name={range_name}")
    service = get_sheets_service()
    log.debug(f"Calling Sheets API with spreadsheetId={spreadsheet_id}, range={range_name}")
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    values = result.get("values", [])
    log.debug(f"Sheets API returned {len(values)} rows")
    return values


def write_sheet(spreadsheet_id, range_name, values=None):
    log.debug(
        f"write_sheet called with spreadsheet_id={spreadsheet_id}, range_name={range_name}, values length={len(values) if values else 0}"
    )
    if values:
        preview = values[:3] if len(values) > 3 else values
        log.debug(f"Preview of values to write: {preview}")
    else:
        log.debug("No values provided to write.")
    service = get_sheets_service()
    log.debug(f"Calling Sheets API with spreadsheetId={spreadsheet_id}, range={range_name}")
    body = {"values": values}
    result = (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body=body,
        )
        .execute()
    )
    log.info(f"write_sheet updated range {range_name} with {len(values) if values else 0} rows")
    return result


def append_rows(spreadsheet_id: str, range_name: str, values: list) -> None:
    log.debug(
        f"append_rows called with spreadsheet_id={spreadsheet_id}, range_name={range_name}, number of rows={len(values)}"
    )
    service = get_sheets_service()
    body = {"values": values}
    log.debug("Calling Sheets API to append rows...")
    result = (
        service.spreadsheets()
        .values()
        .append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body,
        )
        .execute()
    )
    log.info(f"Appended {len(values)} rows to {range_name} with result: {result}")


# Utility logging functions to handle writing to specific tabs
def log_debug(spreadsheet_id: str, message: str):
    log.debug(f"log_debug called with spreadsheet_id={spreadsheet_id}, message={message}")
    get_or_create_sheet(spreadsheet_id, "Debug")
    log.debug(f"Logging to Debug: {message}")
    append_rows(spreadsheet_id, "Debug!A1", [[message]])


def log_info(spreadsheet_id: str, message: str):
    log.debug(f"log_info called with spreadsheet_id={spreadsheet_id}, message={message}")
    get_or_create_sheet(spreadsheet_id, "Info")
    log.info(f"Logging to Info: {message}")
    append_rows(spreadsheet_id, "Info!A1", [[message]])


# Ensure all required sheet tabs exist in a spreadsheet
def ensure_sheet_exists(spreadsheet_id: str, sheet_name: str, headers: list[str] = None) -> None:
    log.debug(
        f"ensure_sheet_exists called with spreadsheet_id={spreadsheet_id}, sheet_name={sheet_name}, headers={headers}"
    )
    get_or_create_sheet(spreadsheet_id, sheet_name)
    if headers:
        existing = read_sheet(spreadsheet_id, f"{sheet_name}!1:1")
        if not existing:
            write_sheet(spreadsheet_id, f"{sheet_name}!A1", [headers])
            log.info(f"Wrote headers to sheet '{sheet_name}': {headers}")
        else:
            log.debug(f"Headers already present in sheet '{sheet_name}'; no write needed.")


# Function to fetch spreadsheet metadata
def get_sheet_metadata(spreadsheet_id: str):
    log.debug(f"get_sheet_metadata called with spreadsheet_id={spreadsheet_id}")
    service = get_sheets_service()
    log.debug(f"Fetching spreadsheet metadata for ID={spreadsheet_id}")
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    log.info(f"Metadata keys available: {list(metadata.keys())}")
    return metadata


def update_row(spreadsheet_id: str, range_: str, values: list[list[str]]):
    """
    Update a specific row in a Google Sheet.

    Args:
        spreadsheet_id (str): The ID of the spreadsheet.
        range_ (str): The A1-style range to update (e.g., "Processed!A2:C2").
        values (list[list[str]]): 2D list of values to set in the range.

    Example:
        update_row(
            "spreadsheet_id_here",
            "Processed!A2:C2",
            [["filename.m3u", "2025-10-09", "last_extvdj_line"]]
        )
    """
    service = get_sheets_service()
    body = {"values": values}
    result = (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=range_,
            valueInputOption="USER_ENTERED",
            body=body,
        )
        .execute()
    )
    return result


def sort_sheet_by_column(
    spreadsheet_id: str,
    sheet_name: str,
    column_index: int,
    ascending: bool = True,
    start_row: int = 2,
    end_row: int = None,
):
    """
    Sort a sheet by a specific column.

    Args:
        spreadsheet_id (str): The ID of the spreadsheet.
        sheet_name (str): The name of the sheet tab.
        column_index (int): The 0-based column index to sort by.
        ascending (bool): Whether to sort ascending (True) or descending (False).
        start_row (int): Row index (1-based in UI, 0-based in API) to start sorting.
                         Default is 2 to skip header row.
        end_row (int): Optional end row index (exclusive). If None, will sort until the last row.
    """
    log.debug(
        f"sort_sheet_by_column called with spreadsheet_id={spreadsheet_id}, "
        f"sheet_name={sheet_name}, column_index={column_index}, ascending={ascending}, "
        f"start_row={start_row}, end_row={end_row}"
    )
    service = get_sheets_service()

    # Get sheet ID from metadata
    metadata = get_sheet_metadata(spreadsheet_id)
    sheet_id = None
    for sheet in metadata.get("sheets", []):
        if sheet["properties"]["title"] == sheet_name:
            sheet_id = sheet["properties"]["sheetId"]
            break

    if sheet_id is None:
        raise ValueError(f"Sheet '{sheet_name}' not found in spreadsheet {spreadsheet_id}")

    sort_spec = {
        "dimensionIndex": column_index,
        "sortOrder": "ASCENDING" if ascending else "DESCENDING",
    }

    sort_range = {
        "sheetId": sheet_id,
        "startRowIndex": start_row - 1,  # Convert to 0-based
    }
    if end_row is not None:
        sort_range["endRowIndex"] = end_row

    request_body = {"requests": [{"sortRange": {"range": sort_range, "sortSpecs": [sort_spec]}}]}

    log.info(
        f"Sorting sheet '{sheet_name}' on column {column_index} ({'ASC' if ascending else 'DESC'})"
    )
    result = (
        service.spreadsheets()
        .batchUpdate(spreadsheetId=spreadsheet_id, body=request_body)
        .execute()
    )
    log.debug("Batch update executed")
    return result


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
