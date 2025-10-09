
import io
import pytz
import datetime
from googleapiclient.http import MediaIoBaseDownload
from core import logger as log
from tools.live_music_history import config

log = log.get_logger()


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

def get_most_recent_m3u_file(drive_service):
    log.info("Fetching most recent .m3u file from Drive...")
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
        log.info("No .m3u files found in Drive folder.")
        return None
    files.sort(key=lambda f: f["name"])
    recent_file = files[-1]
    log.info("Most recent .m3u file found: %s", recent_file["name"])
    return recent_file


def download_m3u_file(drive_service, file_id):
    log.info("Downloading .m3u file with ID: %s", file_id)
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    lines = fh.getvalue().decode("utf-8").splitlines()
    log.info("Downloaded and read %d lines from .m3u file.", len(lines))
    return lines


def parse_m3u_lines(lines, existing_keys, file_date_str):
    log.info("Parsing .m3u lines to extract entries...")
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
    log.info("Parsed %d new entries from .m3u file.", len(entries))
    return entries

