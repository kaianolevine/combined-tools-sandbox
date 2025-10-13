import pytz
import datetime
from urllib.parse import urlencode
from core import logger
import config
import core.m3u_parsing as m3u_parsing
from core import google_drive
from core import google_sheets

log = logger.get_logger()
# from __future__ import print_function
# def log(msg, *args):
#    print(msg % args if args else msg)


def build_youtube_links(entries):
    links = []
    for _, title, artist in entries:
        query = urlencode({"search_query": f"{title} {artist}"})
        url = f"https://www.youtube.com/results?{query}"
        links.append([f'=HYPERLINK("{url}", "YouTube Search")'])
    return links


def write_entries_to_sheet(sheets_service, entries, now):
    sheet = sheets_service.spreadsheets()
    log.info("Clearing old entries in sheet range A5:D...")
    sheet.values().clear(spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID, range="A5:D").execute()

    if not entries:
        log.info("No entries to write. Writing NO_HISTORY message.")
        sheet.values().update(
            spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID,
            range="A5:B5",
            valueInputOption="RAW",
            body={"values": [[logger.format_date(now), config.NO_HISTORY]]},
        ).execute()
        return

    log.info("Writing %d entries to sheet...", len(entries))
    sheet.values().update(
        spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID,
        range=f"A5:C{5+len(entries)-1}",
        valueInputOption="RAW",
        body={"values": entries},
    ).execute()

    links = build_youtube_links(entries)
    log.info("Writing YouTube links for entries...")
    sheet.values().update(
        spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID,
        range=f"D5:D{5+len(links)-1}",
        valueInputOption="USER_ENTERED",
        body={"values": links},
    ).execute()
    log.info("Finished writing entries and links to sheet.")


# --- SHEET READING AND PUBLISHING HISTORY ---


def read_existing_entries(sheets_service, cutoff):
    sheet = sheets_service.spreadsheets()
    log.info("Reading existing entries from sheet...")
    result = (
        sheet.values()
        .get(spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID, range="A5:C")
        .execute()
    )
    values = result.get("values", [])
    existing_data = []
    for row in values:
        if len(row) >= 2 and row[1] != config.NO_HISTORY:
            try:
                dt = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M")
                if dt >= cutoff:
                    existing_data.append(row[:3])
            except Exception:
                pass
    log.info("Found %d existing entries after cutoff.", len(existing_data))
    return existing_data


def update_last_run_time(sheets_service, now):
    sheet = sheets_service.spreadsheets()
    log.info("Updating last run time in sheet...")
    sheet.values().update(
        spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID,
        range="A3",
        valueInputOption="RAW",
        body={"values": [[logger.format_date(now)]]},
    ).execute()


def publish_history(drive_service, sheets_service):
    tz = pytz.timezone(config.TIMEZONE)
    now = datetime.datetime.now(tz)
    cutoff = now - datetime.timedelta(hours=config.HISTORY_IN_HOURS)

    log.info("--- Starting publish_history ---")

    update_last_run_time(sheets_service, now)

    m3u_file = m3u_parsing.get_most_recent_m3u_file(drive_service)
    if not m3u_file:
        log.info("No .m3u files found. Clearing sheet and writing NO_HISTORY.")
        sheet = sheets_service.spreadsheets()
        sheet.values().clear(
            spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID, range="A5:D"
        ).execute()
        sheet.values().update(
            spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID,
            range="A5:B5",
            valueInputOption="RAW",
            body={"values": [[logger.format_date(now), config.NO_HISTORY]]},
        ).execute()
        return

    lines = m3u_parsing.download_m3u_file(drive_service, m3u_file["id"])
    file_date_str = m3u_file["name"].replace(".m3u", "").strip()

    existing_data = read_existing_entries(sheets_service, cutoff)
    existing_keys = {"||".join(c.strip().lower() for c in r) for r in existing_data}

    new_entries = m3u_parsing.parse_m3u_lines(lines, existing_keys, file_date_str)
    new_entries = [
        r
        for r in new_entries
        if tz.localize(datetime.datetime.strptime(r[0], "%Y-%m-%d %H:%M")) >= cutoff
    ]

    combined = existing_data + new_entries
    log.info("Total combined entries to write: %d", len(combined))

    write_entries_to_sheet(sheets_service, combined, now)

    log.info("Script finished. Rows written: %d", len(combined))


if __name__ == "__main__":
    import tools.private_history.update_private_history as private_history

    drive_service = google_drive.get_drive_service()
    sheets_service = google_sheets.get_sheets_service()

    publish_history(drive_service, sheets_service)
    private_history.publish_private_history(drive_service, sheets_service)
