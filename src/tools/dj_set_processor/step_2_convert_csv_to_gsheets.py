import logging
import os
import re
import time
from datetime import datetime, timezone

from tools.westie_radio.drive import (
    get_drive_service,
    get_or_create_folder,
    get_files_in_folder,
    move_file_to_folder,
    rename_file,
    list_subfolders,
)
from tools.westie_radio.sheets import (
    create_spreadsheet,
    write_to_sheet,
)
from tools.westie_radio.locks import try_lock_folder, release_folder_lock

MIN_FILE_AGE_MINUTES = 1
MAX_RUNTIME_SECONDS = 5 * 60
DESIRED_ORDER = [
    "Label",
    "Title",
    "Remix",
    "Artist",
    "Comment",
    "Genre",
    "Length",
    "Bpm",
    "Year",
    "User 1",
    "User 2",
    "Play Time",
]


def convert_csvs_to_google_sheets_and_archive(root_folder_id):
    drive_service = get_drive_service()
    start_time = time.time()
    process_folder_for_csvs(drive_service, root_folder_id, start_time)


def process_folder_for_csvs(drive_service, folder_id, start_time):
    folder_name = drive_service.files().get(fileId=folder_id, fields="name").execute()["name"]
    if not try_lock_folder(folder_name):
        logging.info(f'🔒 Folder "{folder_name}" is already being processed. Skipping.')
        return

    try:
        files = get_files_in_folder(drive_service, folder_id)
        for file in files:
            if time.time() - start_time > MAX_RUNTIME_SECONDS:
                logging.info("⏸️ Time limit hit. Ending this run.")
                return

            file_id = file["id"]
            file_name = file["name"].strip()
            created = file["createdTime"]
            age_minutes = (
                datetime.now(timezone.utc) - datetime.fromisoformat(created)
            ).total_seconds() / 60
            if age_minutes < MIN_FILE_AGE_MINUTES:
                logging.info(
                    f'⏳ Skipping "{file_name}" — created only {age_minutes:.1f} minute(s) ago.'
                )
                continue

            if not file_name.endswith("_Cleaned.csv") or file_name.startswith("FAILED_"):
                continue

            try:
                logging.info(f"🔄 Converting file: {file_name}")
                content = drive_service.files().get_media(fileId=file_id).execute().decode("utf-8")
                lines = content.replace("\r\n", "\n").split("\n")
                if lines[0].lower().startswith("sep="):
                    lines = lines[1:]

                rows = []
                for line in lines:
                    if not line.strip():
                        continue
                    protected = re.sub(
                        r'"([^"]*?)"', lambda m: f'"{m.group(1).replace(",", "_")}"', line
                    )
                    fields = [
                        cell.strip().strip('"').replace('"', "'") for cell in protected.split(",")
                    ]
                    rows.append(fields)

                if not rows:
                    logging.warning(f"⚠️ Skipping empty/unreadable CSV: {file_name}")
                    continue

                max_cols = max(len(r) for r in rows)
                normalized = [r + [""] * (max_cols - len(r)) for r in rows]

                header = normalized[0]
                col_idx_map = {}
                for col in DESIRED_ORDER:
                    for i, h in enumerate(header):
                        if h.strip().lower() == col.lower():
                            col_idx_map[col] = i
                            break

                reordered = []
                for row in normalized:
                    ordered = [
                        row[col_idx_map[col]] if col in col_idx_map else ""
                        for col in DESIRED_ORDER
                    ]
                    extras = [cell for i, cell in enumerate(row) if i not in col_idx_map.values()]
                    reordered.append(ordered + extras)

                sheet_name = re.sub(r"_Cleaned\.csv$", "", file_name)[:99]
                spreadsheet_id = create_spreadsheet(sheet_name)
                write_to_sheet(spreadsheet_id, reordered)

                year_match = re.match(r"^(\d{4})", file_name)
                if not year_match:
                    logging.warning(f"❌ Could not extract year from filename: {file_name}")
                    continue

                year = year_match.group(1)
                parent = (
                    drive_service.files()
                    .get(fileId=folder_id, fields="parents")
                    .execute()["parents"][0]
                )
                year_folder = get_or_create_folder(drive_service, parent, year)
                csvs_folder = get_or_create_folder(drive_service, year_folder, "CSVs")

                move_file_to_folder(drive_service, file_id, csvs_folder, remove_from=folder_id)
                logging.info(f"✅ Converted and moved: {file_name} → {sheet_name}")
            except Exception as e:
                failed_name = f"FAILED_{file_name}"
                rename_file(drive_service, file_id, failed_name)
                logging.error(f'❌ Conversion failed. Renamed to "{failed_name}": {e}')
                continue

        for subfolder in list_subfolders(drive_service, folder_id):
            if time.time() - start_time > MAX_RUNTIME_SECONDS:
                logging.info("⏸️ Time limit hit during subfolder scan. Ending.")
                return
            name = subfolder["name"]
            if name not in ["CSVs", "Archive"]:
                process_folder_for_csvs(drive_service, subfolder["id"], start_time)

    finally:
        release_folder_lock(folder_name)
