import io
import pytz
import datetime
from googleapiclient.http import MediaIoBaseDownload
from core import logger as log
from tools.live_music_history import config

log = log.get_logger()


def parse_time_str(time_str):
    log.debug(f"parse_time_str called with time_str: '{time_str}'")
    try:
        h, m = map(int, time_str.split(":"))
        minutes = h * 60 + m
        log.debug(f"Parsed time '{time_str}' into {minutes} minutes")
        return minutes
    except Exception as e:
        log.error(f"Error parsing time string '{time_str}': {e}")
        return 0


def extract_tag_value(line, tag):
    import re

    log.debug(f"extract_tag_value called with tag: '{tag}' in line: '{line.strip()}'")
    match = re.search(rf"<{tag}>(.*?)</{tag}>", line, re.I)
    if match:
        value = match.group(1).strip()
        log.debug(f"Found value for tag '{tag}': '{value}'")
        return value
    else:
        log.debug(f"No match found for tag '{tag}'")
        return ""


def get_most_recent_m3u_file(drive_service):
    log.info(f"Fetching most recent .m3u file from Drive folder ID: {config.FOLDER_ID}")
    results = (
        drive_service.files()
        .list(
            q=f"'{config.FOLDER_ID}' in parents and name contains '.m3u' and trashed = false",
            fields="files(id, name)",
        )
        .execute()
    )
    files = results.get("files", [])
    log.info(f"Number of .m3u files found: {len(files)}")
    if files:
        file_names = [f["name"] for f in files]
        log.debug(f"Files found: {file_names}")
    if not files:
        log.info("No .m3u files found in Drive folder.")
        return None
    files.sort(key=lambda f: f["name"])
    recent_file = files[-1]
    log.info(f"Most recent .m3u file found: {recent_file['name']}")
    return recent_file


def download_m3u_file(drive_service, file_id):
    log.info(f"Starting download of .m3u file with ID: {file_id}")
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            log.debug(f"Download progress: {int(status.progress() * 100)}%")
    lines = fh.getvalue().decode("utf-8").splitlines()
    log.info(f"Downloaded and read {len(lines)} lines from .m3u file.")
    return lines


def parse_m3u_lines(lines, existing_keys, file_date_str):
    log.info(f"Parsing .m3u lines to extract entries with file_date_str: {file_date_str}")
    log.debug(f"Total lines received for parsing: {len(lines)}")
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
            log.debug(f"Extracted tags - time: '{time}', title: '{title}', artist: '{artist}'")

            if time and title:
                current_minutes = parse_time_str(time)
                if prev_minutes > -1 and current_minutes < prev_minutes:
                    log.debug(
                        f"Day rollover detected: previous minutes {prev_minutes}, current minutes {current_minutes}"
                    )
                    current_date += datetime.timedelta(days=1)
                prev_minutes = current_minutes

                full_dt = f"{current_date.strftime('%Y-%m-%d')} {time.strip()}"
                key = "||".join(v.strip().lower() for v in [full_dt, title, artist])
                if key not in existing_keys:
                    entries.append([full_dt, title.strip(), artist.strip()])
                    existing_keys.add(key)
                    log.debug(f"Appended new entry: {entries[-1]}")
    log.info(f"Parsed {len(entries)} new entries from .m3u file.")
    return entries
