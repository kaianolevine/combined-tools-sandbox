import pytz
import datetime
from core import logger
import config
import core.m3u_parsing as m3u_parsing
from core import google_drive
from core import google_sheets

log = logger.get_logger()


def read_existing_entries(sheets_service):
    sheet = sheets_service.spreadsheets()
    log.info("Reading existing entries from private history sheet...")
    result = (
        sheet.values()
        .get(spreadsheetId=config.PRIVATE_HISTORY_SPREADSHEET_ID, range="A2:F")
        .execute()
    )
    values = result.get("values", [])
    existing_data = []
    for row in values:
        if len(row) >= 6:
            existing_data.append(row[:6])
    log.info("Found %d existing entries in private history.", len(existing_data))
    return existing_data


def write_entries_to_sheet(sheets_service, entries, now):
    sheet = sheets_service.spreadsheets()
    log.info("Clearing old entries in private history sheet range A2:F...")
    sheet.values().clear(
        spreadsheetId=config.PRIVATE_HISTORY_SPREADSHEET_ID, range="A2:F"
    ).execute()

    if not entries:
        log.info("No entries to write in private history. Writing NO_HISTORY message.")
        sheet.values().update(
            spreadsheetId=config.PRIVATE_HISTORY_SPREADSHEET_ID,
            range="A2:B2",
            valueInputOption="RAW",
            body={"values": [[logger.format_date(now), config.NO_HISTORY, "", "", "", ""]]},
        ).execute()
        return

    log.info("Writing %d entries to private history sheet...", len(entries))
    sheet.values().update(
        spreadsheetId=config.PRIVATE_HISTORY_SPREADSHEET_ID,
        range=f"A2:F{2+len(entries)-1}",
        valueInputOption="RAW",
        body={"values": entries},
    ).execute()

    log.info("Updating last run time in A1 of private history sheet...")
    sheet.values().update(
        spreadsheetId=config.PRIVATE_HISTORY_SPREADSHEET_ID,
        range="A1",
        valueInputOption="RAW",
        body={"values": [[logger.format_date(now)]]},
    ).execute()
    log.info("Finished writing entries and updating timestamp.")


def publish_private_history(drive_service, sheets_service):
    tz = pytz.timezone(config.TIMEZONE)
    now = datetime.datetime.now(tz)

    log.info("--- Starting publish_private_history ---")

    m3u_file = m3u_parsing.get_most_recent_m3u_file(drive_service)
    if not m3u_file:
        log.info("No .m3u files found for private history. Clearing sheet and writing NO_HISTORY.")
        sheet = sheets_service.spreadsheets()
        sheet.values().clear(
            spreadsheetId=config.PRIVATE_HISTORY_SPREADSHEET_ID, range="A2:F"
        ).execute()
        sheet.values().update(
            spreadsheetId=config.PRIVATE_HISTORY_SPREADSHEET_ID,
            range="A2:B2",
            valueInputOption="RAW",
            body={"values": [[logger.format_date(now), config.NO_HISTORY, "", "", "", ""]]},
        ).execute()
        sheet.values().update(
            spreadsheetId=config.PRIVATE_HISTORY_SPREADSHEET_ID,
            range="A1",
            valueInputOption="RAW",
            body={"values": [[logger.format_date(now)]]},
        ).execute()
        return

    lines = m3u_parsing.download_m3u_file(drive_service, m3u_file["id"])
    file_date_str = m3u_file["name"].replace(".m3u", "").strip()

    existing_data = read_existing_entries(sheets_service)
    existing_keys = {"||".join(c.strip().lower() for c in r) for r in existing_data}

    new_entries = m3u_parsing.parse_m3u_lines(lines, existing_keys, file_date_str)

    combined = existing_data + new_entries

    # Sort descending by first column as datetime string
    def safe_sort_key(row):
        try:
            return datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M")
        except Exception:
            return datetime.datetime.min

    combined.sort(key=safe_sort_key, reverse=True)

    # Keep only top 200 entries
    trimmed = combined[:200]

    log.info("Total combined entries after trimming to 200: %d", len(trimmed))

    write_entries_to_sheet(sheets_service, trimmed, now)

    log.info("Script finished. Rows written: %d", len(trimmed))


if __name__ == "__main__":

    drive_service = google_drive.get_drive_service()
    sheets_service = google_sheets.get_sheets_service()

    publish_private_history(drive_service, sheets_service)
