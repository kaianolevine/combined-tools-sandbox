import io
import pytz
import datetime
from googleapiclient.http import MediaIoBaseDownload
from urllib.parse import urlencode
from tools.live_music_history import config
from core import logger as log

log = log.get_logger()
# def log(msg, *args):
#    print(msg % args if args else msg)


def format_date(dt: datetime.datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")


def parse_time_str(time_str):
    try:
        h, m = map(int, time_str.split(":"))
        return h * 60 + m
    except Exception:
        return 0


def extract_tag_value(line, tag):
    import re

    match = re.search(rf"<{tag}>(.*?)</{tag}>", line, re.I)
    return match.group(1).strip() if match else ""


def build_youtube_links(entries):
    links = []
    for _, title, artist in entries:
        query = urlencode({"search_query": f"{title} {artist}"})
        url = f"https://www.youtube.com/results?{query}"
        links.append([f'=HYPERLINK("{url}", "YouTube Search")'])
    return links


def get_most_recent_m3u_file(drive_service):
    results = (
        drive_service.files()
        .list(
            q=f"'{config.FOLDER_ID}' in parents and name contains '.m3u' and trashed = false",
            fields="files(id, name)",
        )
        .execute()
    )
    files = results.get("files", [])
    if not files:
        return None
    files.sort(key=lambda f: f["name"])
    return files[-1]


def parse_m3u_lines(lines, existing_keys, file_date_str):
    tz = pytz.timezone(config.TIMEZONE)
    year, month, day = map(int, file_date_str.split("-"))
    current_date = datetime.datetime(year, month, day, tzinfo=tz)
    prev_minutes = -1
    entries = []

    for line in lines:
        if line.strip().lower().startswith("#extvdj:"):
            time = extract_tag_value(line, "time")
            title = extract_tag_value(line, "title")
            artist = extract_tag_value(line, "artist") or ""

            if time and title:
                current_minutes = parse_time_str(time)
                if prev_minutes > -1 and current_minutes < prev_minutes:
                    current_date += datetime.timedelta(days=1)
                prev_minutes = current_minutes

                full_dt = f"{current_date.strftime('%Y-%m-%d')} {time.strip()}"
                key = "||".join(v.strip().lower() for v in [full_dt, title, artist])
                if key not in existing_keys:
                    entries.append([full_dt, title.strip(), artist.strip()])
                    existing_keys.add(key)
    return entries


def parse_m3u_and_insert_to_sheet(drive_service, sheets_service):
    tz = pytz.timezone(config.TIMEZONE)
    now = datetime.datetime.now(tz)
    cutoff = now - datetime.timedelta(hours=config.HISTORY_IN_HOURS)

    log("--- Starting parseM3UAndInsertToSheet ---")
    m3u_file = get_most_recent_m3u_file()
    sheet = sheets_service.spreadsheets()

    # update last run time
    sheet.values().update(
        spreadsheetId=config.SHEET_ID,
        range="A3",
        valueInputOption="RAW",
        body={"values": [[format_date(now)]]},
    ).execute()

    if not m3u_file:
        log("No .m3u files found.")
        sheet.values().clear(spreadsheetId=config.SHEET_ID, range="A5:D").execute()
        sheet.values().update(
            spreadsheetId=config.SHEET_ID,
            range="A5:B5",
            valueInputOption="RAW",
            body={"values": [[format_date(now), config.NO_HISTORY]]},
        ).execute()
        return

    log("Found most recent .m3u file: %s", m3u_file["name"])
    file_id = m3u_file["id"]
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    lines = fh.getvalue().decode("utf-8").splitlines()

    file_date_str = m3u_file["name"].replace(".m3u", "").strip()

    # read existing
    result = sheet.values().get(spreadsheetId=config.SHEET_ID, range="A5:C").execute()
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
    existing_keys = {"||".join(c.strip().lower() for c in r) for r in existing_data}

    # parse new
    new_entries = parse_m3u_lines(lines, existing_keys, file_date_str)
    new_entries = [
        r
        for r in new_entries
        if tz.localize(datetime.datetime.strptime(r[0], "%Y-%m-%d %H:%M")) >= cutoff
    ]
    combined = existing_data + new_entries

    # clear old
    sheet.values().clear(spreadsheetId=config.SHEET_ID, range="A5:D").execute()

    if not combined:
        log("No recent entries.")
        sheet.values().update(
            spreadsheetId=config.SHEET_ID,
            range="A5:B5",
            valueInputOption="RAW",
            body={"values": [[format_date(now), config.NO_HISTORY]]},
        ).execute()
        return

    # write values
    sheet.values().update(
        spreadsheetId=config.SHEET_ID,
        range=f"A5:C{5+len(combined)-1}",
        valueInputOption="RAW",
        body={"values": combined},
    ).execute()

    # write links
    links = build_youtube_links(combined)
    sheet.values().update(
        spreadsheetId=config.SHEET_ID,
        range=f"D5:D{5+len(links)-1}",
        valueInputOption="USER_ENTERED",
        body={"values": links},
    ).execute()

    log("Script finished. Rows written: %d", len(combined))
