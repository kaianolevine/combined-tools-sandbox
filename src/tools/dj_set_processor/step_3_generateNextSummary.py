import os
import logging
from your_package.westie_radio.drive import (
    get_drive_service,
    get_folder_by_name,
    get_files_in_folder,
    create_folder_if_missing,
    move_file_to_folder,
)
from your_package.westie_radio.sheets import (
    create_spreadsheet,
    get_sheet_data,
    write_sheet_data,
    format_summary_sheet,
)
from your_package.westie_radio.locks import try_lock_folder, release_folder_lock

DJ_SETS_FOLDER_ID = os.environ.get("DJ_SETS_FOLDER_ID")
SUMMARY_FOLDER_NAME = "Summary"
ALLOWED_HEADERS = ["title", "artist", "remix", "comment", "genre", "length", "bpm", "year"]


def generate_next_missing_summary():
    logging.info("🚀 Starting generate_next_missing_summary()")
    drive_service = get_drive_service()
    parent_folder = (
        drive_service.files().get(fileId=DJ_SETS_FOLDER_ID, fields="id, name").execute()
    )

    summary_folder = create_folder_if_missing(
        drive_service, DJ_SETS_FOLDER_ID, SUMMARY_FOLDER_NAME
    )

    if not try_lock_folder(SUMMARY_FOLDER_NAME):
        logging.info("🔒 Summary generation is locked. Skipping run.")
        return

    year_folders = get_files_in_folder(
        drive_service, DJ_SETS_FOLDER_ID, mime_type="application/vnd.google-apps.folder"
    )
    for folder in year_folders:
        year = folder["name"]
        if year.lower() == "summary":
            continue

        pending_name = f"_pending_{year} Summary"
        summary_name = f"{year} Summary"
        existing_summaries = get_files_in_folder(
            drive_service, summary_folder["id"], name_contains=summary_name
        )
        if existing_summaries:
            logging.info(f"✅ Summary already exists for {year}")
            continue

        files = get_files_in_folder(
            drive_service, folder["id"], mime_type="application/vnd.google-apps.spreadsheet"
        )
        if any(f["name"].startswith("FAILED_") or "_Cleaned" in f["name"] for f in files):
            logging.info(f"⛔ Skipping year {year} — unready files found")
            continue

        if not try_lock_folder(year):
            logging.info(f"🔒 Year {year} is locked. Skipping.")
            continue

        try:
            logging.info(f"🔧 Generating summary for {year}...")
            generate_summary_for_folder(
                drive_service, files, summary_folder["id"], summary_name, year
            )
        finally:
            release_folder_lock(year)
            release_folder_lock(SUMMARY_FOLDER_NAME)
        break  # Only generate one per run


def generate_summary_for_folder(drive_service, files, summary_folder_id, summary_name, year):
    all_headers = set()
    sheet_data = []

    for f in files:
        logging.info(f"🔍 Reading {f['name']}")
        sheets = get_sheet_data(f["id"])  # Should return list of (header, rows)
        for header, rows in sheets:
            lower_header = [h.strip().lower() for h in header]
            keep_indices = [i for i, h in enumerate(lower_header) if h in ALLOWED_HEADERS]
            if not keep_indices:
                continue
            filtered_header = [header[i] for i in keep_indices]
            filtered_rows = [
                [row[i] for i in keep_indices] for row in rows if any(cell.strip() for cell in row)
            ]
            if filtered_rows:
                all_headers.update(filtered_header)
                sheet_data.append((filtered_header, filtered_rows))

    if not sheet_data:
        logging.info(f"📭 No valid data found in folder: {year}")
        return

    final_header = list(all_headers) + ["Count"]
    final_rows = []
    for header, rows in sheet_data:
        idx_map = {h: i for i, h in enumerate(header)}
        for row in rows:
            aligned = [row[idx_map[h]] if h in idx_map else "" for h in final_header[:-1]]
            final_rows.append(aligned + [1])

    if "Title" in final_header:
        title_index = final_header.index("Title")
        final_rows.sort(key=lambda r: r[title_index])
    else:
        final_rows.sort()

    ss_id = create_spreadsheet(summary_name)
    write_sheet_data(ss_id, "Summary", final_header, final_rows)
    format_summary_sheet(ss_id, "Summary", final_header, final_rows)

    move_file_to_folder(drive_service, ss_id, summary_folder_id)
    logging.info(f"✅ Summary successfully created: {summary_name}")
