import logging
from googleapiclient.discovery import build
from google.oauth2 import service_account
from tools.westie_radio import config

logger = logging.getLogger(__name__)


def get_sheets_service():
    keyfile_path = config.GOOGLE_APPLICATION_CREDENTIALS

    credentials = service_account.Credentials.from_service_account_file(
        keyfile_path, scopes=config.SCOPES
    )
    service = build("sheets", "v4", credentials=credentials)
    return service


def get_or_create_sheet(spreadsheet_id: str, sheet_name: str) -> None:
    service = get_sheets_service()
    sheets_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_titles = [s["properties"]["title"] for s in sheets_metadata.get("sheets", [])]
    if sheet_name not in sheet_titles:
        logger.debug(f"🧪 Creating new sheet tab: {sheet_name}")
        add_sheet_body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body=add_sheet_body
        ).execute()


def read_sheet(spreadsheet_id, range_name):
    logger.debug(f"Reading sheet: ID={spreadsheet_id}, Range={range_name}")
    service = get_sheets_service()
    logger.debug(f"🧪 Calling Sheets API with spreadsheetId={spreadsheet_id}, range={range_name}")
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return result.get("values", [])


def write_sheet(spreadsheet_id, range_name, values=None):
    logger.debug(f"Writing sheet: ID={spreadsheet_id}, Range={range_name}")
    service = get_sheets_service()
    logger.debug(f"🧪 Calling Sheets API with spreadsheetId={spreadsheet_id}, range={range_name}")
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
    return result


def append_rows(spreadsheet_id: str, range_name: str, values: list) -> None:
    service = get_sheets_service()
    body = {"values": values}
    logger.debug("🧪 Calling Sheets API to append rows...")
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
    logger.debug("✅ Appended rows result: %s", result)


# Utility logging functions to handle writing to specific tabs
def log_debug(spreadsheet_id: str, message: str):
    # get_or_create_sheet(spreadsheet_id, "Debug")
    logger.debug(f"🧪 Logging to Debug: {message}")
    # append_rows(spreadsheet_id, "Debug!A1", [[message]])
    pass


def log_info(spreadsheet_id: str, message: str):
    get_or_create_sheet(spreadsheet_id, "Info")
    logger.info(f"🧪 Logging to Info: {message}")
    append_rows(spreadsheet_id, "Info!A1", [[message]])


def log_processed(spreadsheet_id: str, filename: str, last_time: str):
    get_or_create_sheet(spreadsheet_id, "Processed")
    logger.info(f"🧪 Logging to Processed: {filename}, LastTime={last_time}")
    append_rows(spreadsheet_id, "Processed!A1", [[filename, last_time]])


def log_processed_full(
    spreadsheet_id: str,
    filename: str,
    timestamp: str,
    last_play_time: str,
    title: str,
    artist: str,
):
    get_or_create_sheet(spreadsheet_id, "Processed")
    logger.info(
        f"🧪 Logging full processed entry: {filename}, Timestamp={timestamp}, LastPlayTime={last_play_time}, Title={title}, Artist={artist}"
    )
    append_rows(
        spreadsheet_id,
        "Processed!A1",
        [[filename, timestamp, last_play_time, title, artist]],
    )


def get_latest_processed(spreadsheet_id: str):
    get_or_create_sheet(spreadsheet_id, "Processed")
    logger.debug("🧪 Reading latest processed entry from Processed tab...")
    values = read_sheet(spreadsheet_id, "Processed!A2:E")
    if not values:
        logger.debug("🧪 No processed entries found.")
        return None
    return values[-1]  # Return the last row (most recent entry)


# Ensure all required sheet tabs exist in a spreadsheet
def ensure_sheet_exists(spreadsheet_id: str, sheet_name: str, headers: list[str] = None) -> None:
    get_or_create_sheet(spreadsheet_id, sheet_name)
    if headers:
        existing = read_sheet(spreadsheet_id, f"{sheet_name}!1:1")
        if not existing:
            write_sheet(spreadsheet_id, f"{sheet_name}!A1", [headers])


def ensure_log_sheet_exists(spreadsheet_id: str) -> None:
    """
    Ensure that the logging sheet contains all required tabs with headers.
    """
    get_or_create_sheet(spreadsheet_id, "Debug")
    get_or_create_sheet(spreadsheet_id, "Info")
    get_or_create_sheet(spreadsheet_id, "Processed")

    # Optionally write headers if the sheet is empty
    existing = read_sheet(spreadsheet_id, "Processed!A1:E1")
    if not existing:
        write_sheet(
            spreadsheet_id,
            "Processed!A1",
            [["Filename", "Date", "LastPlayTime", "Title", "Artist"]],
        )


# Function to fetch spreadsheet metadata
def get_sheet_metadata(spreadsheet_id: str):
    service = get_sheets_service()
    logger.debug(f"🧪 Fetching spreadsheet metadata for ID={spreadsheet_id}")
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    return metadata


# Function to delete a sheet by sheet ID
def delete_sheet_by_id(spreadsheet_id: str, sheet_id: int) -> None:
    service = get_sheets_service()
    logger.debug(f"🧪 Deleting sheet with ID={sheet_id} from spreadsheet ID={spreadsheet_id}")
    request_body = {"requests": [{"deleteSheet": {"sheetId": sheet_id}}]}
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=request_body).execute()
