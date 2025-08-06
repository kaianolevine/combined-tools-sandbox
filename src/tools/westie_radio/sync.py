"""
sync.py — Main integration script for Westie Radio automation.
"""

from your_package.westie_radio import spotify, drive, sheets
from your_package.westie_radio import config
from googleapiclient.errors import HttpError

spreadsheet_id = config.SPREADSHEET_ID


def extract_date_from_filename(filename):
    import re

    match = re.match(r"(\d{4}-\d{2}-\d{2})", filename)
    return match.group(1) if match else filename


def parse_m3u(filepath):
    """Parses .m3u file and returns a list of (artist, title, extvdj_line) tuples."""
    import re

    songs = []
    sheets.log_debug(spreadsheet_id, f"Opening M3U file: {filepath}")
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()
        sheets.log_debug(spreadsheet_id, f"Read {len(lines)} lines from {filepath}")
        for line in lines:
            line = line.strip()
            # sheets.log_debug(spreadsheet_id, f"Stripped line: {line}")
            if line.startswith("#EXTVDJ:"):
                artist_match = re.search(r"<artist>(.*?)</artist>", line)
                title_match = re.search(r"<title>(.*?)</title>", line)
                if artist_match and title_match:
                    artist = artist_match.group(1).strip()
                    title = title_match.group(1).strip()
                    songs.append((artist, title, line))
            #        sheets.log_debug(spreadsheet_id, f"Parsed song - Artist: '{artist}', Title: '{title}'")
            #    else:
            #        sheets.log_debug(spreadsheet_id, f"Missing artist or title in line: {line}")
            # else:
            #    sheets.log_debug(spreadsheet_id, f"Ignored line: {line}")
    sheets.log_debug(spreadsheet_id, f"Total parsed songs: {len(songs)}")
    return songs


def main():
    from datetime import datetime

    sheets.log_info(
        spreadsheet_id,
        f"🔄 Starting Westie Radio sync at {datetime.now().replace(microsecond=0).isoformat()}...",
    )
    sheets.log_debug(spreadsheet_id, "Starting debug logging for Westie Radio sync.")

    # Ensure necessary sheets and remove default 'Sheet1' if present
    initialize_spreadsheet()
    # --- Google Drive: find and download all .m3u files ---
    folder_id = config.M3U_FOLDER_ID
    if not folder_id:
        raise ValueError("Missing environment variable: M3U_FOLDER_ID")
    sheets.log_debug(spreadsheet_id, f"📁 Loaded M3U_FOLDER_ID: {folder_id}")

    all_files = drive.list_files_in_folder(folder_id)
    m3u_files = sorted(
        [f for f in all_files if f["name"].lower().endswith(".m3u")],
        key=lambda f: f["name"],
    )

    if not m3u_files:
        sheets.log_info(spreadsheet_id, "❌ No .m3u files found.")
        return

    # Read processed log once
    processed_rows = sheets.read_sheet(spreadsheet_id, "Processed!A2:C")
    processed_map = {row[0]: row[2] for row in processed_rows if len(row) >= 3}

    for file in m3u_files:
        filename = file["name"]
        file_id = file["id"]
        date = extract_date_from_filename(filename)
        sheets.log_info(spreadsheet_id, f"🎶 Processing file: {filename}")

        drive.download_file(file_id, filename)
        songs = parse_m3u(filename)

        last_extvdj_line = processed_map.get(filename)
        new_songs = songs
        if last_extvdj_line:
            try:
                last_index = [s[2] for s in songs].index(last_extvdj_line)
                new_songs = songs[last_index + 1 :]
                sheets.log_debug(
                    spreadsheet_id,
                    f"⚙️ Skipping {last_index + 1} already-processed songs.",
                )
                if not new_songs:
                    sheets.log_debug(spreadsheet_id, f"🛑 No new songs in {filename}, skipping.")
                    continue
            except ValueError:
                sheets.log_debug(
                    spreadsheet_id,
                    f"⚠️ Last logged song not found in {filename}, processing full file.",
                )

        # --- Spotify: search and collect URIs ---
        found_uris = []
        matched_songs = []
        matched_extvdj_lines = []
        unfound = []
        for artist, title, extvdj_line in new_songs:
            uri = spotify.search_track(artist, title)
            sheets.log_debug(
                spreadsheet_id,
                f"Searching for track - Artist: {artist}, Title: {title}, Found URI: {uri}",
            )
            if uri:
                found_uris.append(uri)
                matched_songs.append((artist, title))
                matched_extvdj_lines.append(extvdj_line)
            else:
                unfound.append((artist, title, extvdj_line))

        sheets.log_info(
            spreadsheet_id,
            f"✅ Found {len(found_uris)} tracks, ❌ {len(unfound)} unfound",
        )

        # --- Add to playlist and trim ---
        try:
            spotify.add_tracks_to_playlist(found_uris)
            spotify.trim_playlist_to_limit()
        except Exception as e:
            sheets.log_debug(spreadsheet_id, f"Error updating Spotify playlist: {e}")

        # --- Log to Google Sheets ---
        sheet = sheets.read_sheet(spreadsheet_id, "Songs Added")
        sheets.log_debug(spreadsheet_id, f"📋 Loaded sheet: {sheet}")

        for (artist, title), uri in zip(matched_songs, found_uris):
            sheets.log_debug(
                spreadsheet_id, f"📝 Would log synced track: {date}, {title} - {artist}"
            )
        rows_to_append = [[date, title, artist] for (artist, title) in matched_songs]
        if rows_to_append:
            sheets.log_debug(spreadsheet_id, f"🧪 Writing {len(rows_to_append)} rows to sheet...")
            try:
                sheets.append_rows(spreadsheet_id, "Songs Added", rows_to_append)
            except Exception as e:
                sheets.log_debug(spreadsheet_id, f"Failed to append to Songs Added: {e}")
        else:
            sheets.log_debug(spreadsheet_id, "🧪 No rows to write to Songs Added.")
        for artist, title, _ in unfound:
            sheets.log_info(
                spreadsheet_id,
                f"❌ Would log unfound track: {date} - {artist} - {title}",
            )

        # Log unfound songs to "Songs Not Found"
        unfound_rows = [[date, title, artist] for artist, title, _ in unfound]
        if unfound_rows:
            try:
                sheets.append_rows(spreadsheet_id, "Songs Not Found", unfound_rows)
            except Exception as e:
                sheets.log_debug(spreadsheet_id, f"Failed to append to Songs Not Found: {e}")

        # --- Log processing summary to "Processed" tab ---
        last_logged_extvdj_line = new_songs[-1][2] if new_songs else last_extvdj_line
        updated_row = [filename, date, last_logged_extvdj_line]
        try:
            all_rows = sheets.read_sheet(spreadsheet_id, "Processed!A2:C")
            filenames = [row[0] for row in all_rows]
            if filename in filenames:
                row_index = filenames.index(filename) + 2  # account for header
                sheets.update_row(
                    spreadsheet_id,
                    f"Processed!A{row_index}:C{row_index}",
                    [updated_row],
                )
            else:
                sheets.append_rows(spreadsheet_id, "Processed", [updated_row])
            sheets.sort_sheet_by_column(
                spreadsheet_id, "Processed", column_index=2, descending=True
            )
        except Exception as e:
            sheets.log_debug(spreadsheet_id, f"Failed to update Processed log: {e}")

    sheets.log_info(spreadsheet_id, "✅ Sync complete.")


def find_file_by_name(folder_id, target_name):
    files = drive.list_files_in_folder(folder_id)
    for f in files:
        if f["name"] == target_name:
            return f
    raise FileNotFoundError(f"File named {target_name} not found in folder {folder_id}")


def initialize_spreadsheet():
    """Ensure necessary sheets exist and remove default 'Sheet1' if present."""

    # Ensure necessary sheets
    sheets.ensure_sheet_exists(
        spreadsheet_id, "Processed", headers=["Filename", "Date", "ExtVDJLine"]
    )
    sheets.ensure_sheet_exists(spreadsheet_id, "Songs Added", headers=["Date", "Title", "Artist"])
    sheets.ensure_sheet_exists(
        spreadsheet_id, "Songs Not Found", headers=["Date", "Title", "Artist"]
    )

    # Attempt to delete 'Sheet1' if it exists
    try:
        metadata = sheets.get_sheet_metadata(spreadsheet_id)
        for sheet_info in metadata.get("sheets", []):
            title = sheet_info.get("properties", {}).get("title", "")
            sheet_id = sheet_info.get("properties", {}).get("sheetId", None)
            if title == "Sheet1" and sheet_id is not None:
                sheets.delete_sheet_by_id(spreadsheet_id, sheet_id)
                sheets.log_debug(spreadsheet_id, "🗑 Deleted default 'Sheet1'.")
    except HttpError as e:
        sheets.log_debug(spreadsheet_id, f"⚠️ Failed to delete 'Sheet1': {e}")


if __name__ == "__main__":
    main()
