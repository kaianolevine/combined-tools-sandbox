import re
from typing import List
import core.google_drive as google_api
import tools.dj_set_processor.config as config
import tools.dj_set_processor.helpers as helpers
from core import logger as log

log = log.get_logger()


def generate_dj_set_collection():
    log.info("🚀 Starting generate_dj_set_collection")
    drive_service = google_api.get_drive_service()
    sheets_service = google_api.get_sheets_service()

    # Locate DJ_SETS folder (we assume the constant ID points to the shared drive folder or folder in shared drive)
    parent_folder_id = config.DJ_SETS
    log.info(f"📁 Using DJ_SETS folder: {parent_folder_id}")

    # Check for existing file or create new (create directly in the shared drive parent)
    spreadsheet_id = google_api.find_or_create_file_by_name(
        drive_service,
        config.OUTPUT_NAME,
        parent_folder_id,
        mime_type="application/vnd.google-apps.spreadsheet",
    )
    log.info(f"📄 Spreadsheet ID: {spreadsheet_id}")

    # Ensure there's exactly one temp sheet to start from
    google_api.clear_all_except_one_sheet(sheets_service, spreadsheet_id, config.TEMP_TAB_NAME)

    # Enumerate subfolders in DJ_SETS
    subfolders = google_api.get_all_subfolders(drive_service, parent_folder_id)
    log.debug(f"Retrieved {len(subfolders)} subfolders")
    subfolders.sort(key=lambda f: f["name"], reverse=True)

    tabs_to_add: List[str] = []

    for folder in subfolders:
        name = folder["name"]
        folder_id = folder["id"]
        log.info(f"📁 Processing folder: {name} (id: {folder_id})")

        files = google_api.get_files_in_folder(drive_service, folder_id)
        log.debug(f"Found {len(files)} files in folder '{name}'")
        rows = []

        for f in files:
            file_name = f.get("name", "")
            mime_type = f.get("mimeType", "")
            file_url = f"https://docs.google.com/spreadsheets/d/{f.get('id', '')}"
            log.debug(f"Processing file: Name='{file_name}', MIME='{mime_type}', URL='{file_url}'")

            if file_name.lower() == "archive":
                log.info(f"⏭️ Skipping folder: {name} (archive folder)")
                continue
            # if mime_type != "application/vnd.google-apps.spreadsheet":
            #    continue

            if name.lower() == "summary":
                year_match = re.match(r"^(\d{4})", file_name)
                year = year_match.group(1) if year_match else ""
                rows.append([year, f'=HYPERLINK("{file_url}", "{file_name}")'])
            else:
                date, title = helpers.extract_date_and_title(file_name)
                rows.append([date, title, f'=HYPERLINK("{file_url}", "{file_name}")'])

        if name.lower() == "summary":
            if rows:
                complete = [r for r in rows if not r[0]]
                others = sorted([r for r in rows if r[0]], key=lambda r: r[0], reverse=True)
                all_rows = complete + others
                log.debug(f"Adding Summary sheet with {len(all_rows)} rows")
                # add Summary sheet
                log.info("➕ Adding Summary sheet")
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={
                        "requests": [
                            {"addSheet": {"properties": {"title": config.SUMMARY_TAB_NAME}}}
                        ]
                    },
                ).execute()
                log.info("Inserting rows into Summary sheet")
                google_api.insert_rows(
                    sheets_service,
                    spreadsheet_id,
                    config.SUMMARY_TAB_NAME,
                    [["Year", "Link"]] + all_rows,
                )
                log.info("Setting column formatting for Summary sheet")
                google_api.set_column_formatting(
                    sheets_service, spreadsheet_id, config.SUMMARY_TAB_NAME, 2
                )
        elif rows:
            rows.sort(key=lambda r: r[0], reverse=True)
            log.debug(f"Adding sheet for folder '{name}' with {len(rows)} rows")
            log.info(f"➕ Adding sheet for folder '{name}'")
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": name}}}]},
            ).execute()
            log.info(f"Inserting rows into sheet '{name}'")
            google_api.insert_rows(
                sheets_service, spreadsheet_id, name, [["Date", "Name", "Link"]] + rows
            )
            log.info(f"Setting column formatting for sheet '{name}'")
            google_api.set_column_formatting(sheets_service, spreadsheet_id, name, 3)
            tabs_to_add.append(name)

    # Clean up temp sheets if any
    log.info(f"Deleting temp sheets: {config.TEMP_TAB_NAME} and 'Sheet1' if they exist")
    google_api.delete_sheet_by_name(sheets_service, spreadsheet_id, config.TEMP_TAB_NAME)
    google_api.delete_sheet_by_name(sheets_service, spreadsheet_id, "Sheet1")

    # Reorder sheets: tabs_to_add then Summary
    log.info(f"Reordering sheets with order: {tabs_to_add + [config.SUMMARY_TAB_NAME]}")
    metadata = google_api.get_spreadsheet_metadata(sheets_service, spreadsheet_id)
    google_api.reorder_sheets(
        sheets_service, spreadsheet_id, tabs_to_add + [config.SUMMARY_TAB_NAME], metadata
    )
    log.info("Completed reordering sheets")

    log.info("✅ Finished generate_dj_set_collection")


if __name__ == "__main__":
    generate_dj_set_collection()
