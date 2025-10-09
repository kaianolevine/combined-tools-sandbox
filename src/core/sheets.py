from core import google_api
from core import logger as log

log = log.get_logger()


def get_sheets_service():
    log.debug("get_sheets_service called")
    service = google_api.get_sheets_service()
    log.debug("Sheets service obtained")
    return service


def get_or_create_sheet(spreadsheet_id: str, sheet_name: str) -> None:
    log.debug(f"get_or_create_sheet called with spreadsheet_id={spreadsheet_id}, sheet_name={sheet_name}")
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
    log.debug(f"write_sheet called with spreadsheet_id={spreadsheet_id}, range_name={range_name}, values length={len(values) if values else 0}")
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
    log.debug(f"append_rows called with spreadsheet_id={spreadsheet_id}, range_name={range_name}, number of rows={len(values)}")
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
    # get_or_create_sheet(spreadsheet_id, "Debug")
    log.debug(f"Logging to Debug: {message}")
    # append_rows(spreadsheet_id, "Debug!A1", [[message]])
    pass


def log_info(spreadsheet_id: str, message: str):
    log.debug(f"log_info called with spreadsheet_id={spreadsheet_id}, message={message}")
    get_or_create_sheet(spreadsheet_id, "Info")
    log.info(f"Logging to Info: {message}")
    append_rows(spreadsheet_id, "Info!A1", [[message]])


def log_processed(spreadsheet_id: str, filename: str, last_time: str):
    log.debug(f"log_processed called with spreadsheet_id={spreadsheet_id}, filename={filename}, last_time={last_time}")
    get_or_create_sheet(spreadsheet_id, "Processed")
    log.info(f"Logging to Processed: {filename}, LastTime={last_time}")
    append_rows(spreadsheet_id, "Processed!A1", [[filename, last_time]])


def log_processed_full(
    spreadsheet_id: str,
    filename: str,
    timestamp: str,
    last_play_time: str,
    title: str,
    artist: str,
):
    log.debug(f"log_processed_full called with spreadsheet_id={spreadsheet_id}, filename={filename}, timestamp={timestamp}, last_play_time={last_play_time}, title={title}, artist={artist}")
    get_or_create_sheet(spreadsheet_id, "Processed")
    log.info(
        f"Logging full processed entry: {filename}, Timestamp={timestamp}, LastPlayTime={last_play_time}, Title={title}, Artist={artist}"
    )
    append_rows(
        spreadsheet_id,
        "Processed!A1",
        [[filename, timestamp, last_play_time, title, artist]],
    )


def get_latest_processed(spreadsheet_id: str):
    log.debug(f"get_latest_processed called with spreadsheet_id={spreadsheet_id}")
    get_or_create_sheet(spreadsheet_id, "Processed")
    log.debug("Reading latest processed entry from Processed tab...")
    values = read_sheet(spreadsheet_id, "Processed!A2:E")
    if not values:
        log.info("No processed entries found.")
        return None
    last_row = values[-1]
    log.info(f"Latest processed entry: {last_row}")
    return last_row  # Return the last row (most recent entry)


# Ensure all required sheet tabs exist in a spreadsheet
def ensure_sheet_exists(spreadsheet_id: str, sheet_name: str, headers: list[str] = None) -> None:
    log.debug(f"ensure_sheet_exists called with spreadsheet_id={spreadsheet_id}, sheet_name={sheet_name}, headers={headers}")
    get_or_create_sheet(spreadsheet_id, sheet_name)
    if headers:
        existing = read_sheet(spreadsheet_id, f"{sheet_name}!1:1")
        if not existing:
            write_sheet(spreadsheet_id, f"{sheet_name}!A1", [headers])
            log.info(f"Wrote headers to sheet '{sheet_name}': {headers}")
        else:
            log.debug(f"Headers already present in sheet '{sheet_name}'; no write needed.")


def ensure_log_sheet_exists(spreadsheet_id: str) -> None:
    """
    Ensure that the logging sheet contains all required tabs with headers.
    """
    log.debug(f"ensure_log_sheet_exists called with spreadsheet_id={spreadsheet_id}")
    get_or_create_sheet(spreadsheet_id, "Debug")
    log.info("Ensured 'Debug' sheet exists.")
    get_or_create_sheet(spreadsheet_id, "Info")
    log.info("Ensured 'Info' sheet exists.")
    get_or_create_sheet(spreadsheet_id, "Processed")
    log.info("Ensured 'Processed' sheet exists.")

    # Optionally write headers if the sheet is empty
    existing = read_sheet(spreadsheet_id, "Processed!A1:E1")
    if not existing:
        write_sheet(
            spreadsheet_id,
            "Processed!A1",
            [["Filename", "Date", "LastPlayTime", "Title", "Artist"]],
        )
        log.info("Wrote headers to 'Processed' sheet.")
    else:
        log.debug("Headers already present in 'Processed' sheet; no write needed.")


# Function to fetch spreadsheet metadata
def get_sheet_metadata(spreadsheet_id: str):
    log.debug(f"get_sheet_metadata called with spreadsheet_id={spreadsheet_id}")
    service = get_sheets_service()
    log.debug(f"Fetching spreadsheet metadata for ID={spreadsheet_id}")
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    log.info(f"Metadata keys available: {list(metadata.keys())}")
    return metadata


# Function to delete a sheet by sheet ID
def delete_sheet_by_id(spreadsheet_id: str, sheet_id: int) -> None:
    log.debug(f"delete_sheet_by_id called with spreadsheet_id={spreadsheet_id}, sheet_id={sheet_id}")
    service = get_sheets_service()
    log.info(f"Deleting sheet with ID={sheet_id} from spreadsheet ID={spreadsheet_id}")
    request_body = {"requests": [{"deleteSheet": {"sheetId": sheet_id}}]}
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=request_body).execute()
    log.info(f"Sheet with ID={sheet_id} deleted successfully from spreadsheet ID={spreadsheet_id}")
