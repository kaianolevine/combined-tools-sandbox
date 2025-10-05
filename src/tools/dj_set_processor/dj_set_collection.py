import re
from typing import List
import tools.dj_set_processor.google_api as google_api
import tools.dj_set_processor.config as config
import tools.dj_set_processor.helpers as helpers


def generate_dj_set_collection():
    config.logger.info("🚀 Starting generate_dj_set_collection")
    drive_service = google_api.get_drive_service()
    sheets_service = google_api.get_sheets_service()

    # Locate DJ_SETS folder (we assume the constant ID points to the shared drive folder or folder in shared drive)
    parent_folder_id = config.DJ_SETS
    config.logger.info(f"📁 Using DJ_SETS folder: {parent_folder_id}")

    # Check for existing file or create new (create directly in the shared drive parent)
    spreadsheet_id = google_api.find_or_create_file_by_name(
        drive_service,
        config.OUTPUT_NAME,
        parent_folder_id,
        mime_type="application/vnd.google-apps.spreadsheet",
    )
    config.logger.info(f"📄 Spreadsheet ID: {spreadsheet_id}")

    # Ensure there's exactly one temp sheet to start from
    google_api.clear_all_except_one_sheet(sheets_service, spreadsheet_id, config.TEMP_TAB_NAME)

    # Enumerate subfolders in DJ_SETS
    subfolders = google_api.get_all_subfolders(drive_service, parent_folder_id)
    subfolders.sort(key=lambda f: f["name"], reverse=True)

    tabs_to_add: List[str] = []

    for folder in subfolders:
        name = folder["name"]
        folder_id = folder["id"]
        config.logger.info(f"📁 Processing folder: {name} (id: {folder_id})")

        files = google_api.get_files_in_folder(drive_service, folder_id)
        rows = []

        for f in files:
            if f.get("mimeType") != "application/vnd.google-apps.spreadsheet":
                continue
            file_name = f["name"]
            file_url = f"https://docs.google.com/spreadsheets/d/{f['id']}"

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
                # add Summary sheet
                config.logger.info("➕ Adding Summary sheet and inserting rows")
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={
                        "requests": [
                            {"addSheet": {"properties": {"title": config.SUMMARY_TAB_NAME}}}
                        ]
                    },
                ).execute()
                google_api.insert_rows(
                    sheets_service,
                    spreadsheet_id,
                    config.SUMMARY_TAB_NAME,
                    [["Year", "Link"]] + all_rows,
                )
                google_api.set_column_formatting(
                    sheets_service, spreadsheet_id, config.SUMMARY_TAB_NAME, 2
                )
        elif rows:
            rows.sort(key=lambda r: r[0], reverse=True)
            config.logger.info(
                f"➕ Adding sheet for folder '{name}' and inserting {len(rows)} rows"
            )
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": name}}}]},
            ).execute()
            google_api.insert_rows(
                sheets_service, spreadsheet_id, name, [["Date", "Name", "Link"]] + rows
            )
            google_api.set_column_formatting(sheets_service, spreadsheet_id, name, 3)
            tabs_to_add.append(name)

    # Clean up temp sheets if any
    google_api.delete_sheet_by_name(sheets_service, spreadsheet_id, config.TEMP_TAB_NAME)
    google_api.delete_sheet_by_name(sheets_service, spreadsheet_id, "Sheet1")

    # Reorder sheets: tabs_to_add then Summary
    metadata = google_api.get_spreadsheet_metadata(sheets_service, spreadsheet_id)
    google_api.reorder_sheets(
        sheets_service, spreadsheet_id, tabs_to_add + [config.SUMMARY_TAB_NAME], metadata
    )

    config.logger.info("✅ Finished generate_dj_set_collection")


if __name__ == "__main__":
    config.logging.basicConfig(level=config.logging.INFO)
    generate_dj_set_collection()
