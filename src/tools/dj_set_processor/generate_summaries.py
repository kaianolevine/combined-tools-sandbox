import re
from collections import OrderedDict

# from difflib import SequenceMatcher
from googleapiclient.errors import HttpError

import google_api
import tools.dj_set_processor.config as config
import tools.dj_set_processor.helpers as helpers


def generate_next_missing_summary():
    """
    Generate the next missing summary for a year, if not locked.
    """
    config.logger.info("🚀 Starting generate_next_missing_summary()")
    drive_service = google_api.get_drive_service()
    sheet_service = google_api.get_sheets_service()

    summary_folder = google_api.get_or_create_folder(
        config.DJ_SETS_FOLDER_ID, config.SUMMARY_FOLDER_NAME, drive_service
    )
    config.logger.debug(f"Summary folder: {summary_folder}")

    # if not helpers.try_lock_folder(config.SUMMARY_FOLDER_NAME):
    #    config.logger.info("🔒 Summary generation is locked. Skipping run.")
    #    return

    year_folders = google_api.get_files_in_folder(
        drive_service, config.DJ_SETS_FOLDER_ID, mime_type="application/vnd.google-apps.folder"
    )
    config.logger.debug(f"Year folders found: {[f['name'] for f in year_folders]}")
    for folder in year_folders:
        year = folder["name"]
        if year.lower() == "summary":
            continue

        summary_name = f"{year} Summary"
        existing_summaries = google_api.get_files_in_folder(
            drive_service, summary_folder, name_contains=summary_name
        )
        config.logger.debug(f"Found existing summaries for {year}: {existing_summaries}")
        if existing_summaries:
            config.logger.info(f"✅ Summary already exists for {year}")
            continue

        config.logger.debug(f"Getting files for year {year}")
        files = google_api.get_files_in_folder(
            drive_service, folder["id"], mime_type="application/vnd.google-apps.spreadsheet"
        )
        if any(f["name"].startswith("FAILED_") or "_Cleaned" in f["name"] for f in files):
            config.logger.info(f"⛔ Skipping year {year} — unready files found")
            continue

        if not helpers.try_lock_folder(year):
            config.logger.info(f"🔒 Year {year} is locked. Skipping.")
            continue

        try:
            config.logger.debug(f"Files to process for {year}: {[f['name'] for f in files]}")
            config.logger.info(f"🔧 Generating summary for {year}...")
            generate_summary_for_folder(
                drive_service, sheet_service, files, summary_folder, summary_name, year
            )
        finally:
            helpers.release_folder_lock(year)
            helpers.release_folder_lock(config.SUMMARY_FOLDER_NAME)


def generate_summary_for_folder(
    drive_service, sheet_service, files, summary_folder_id, summary_name, year
):
    config.logger.debug(
        f"Starting generate_summary_for_folder for year {year} with {len(files)} files"
    )
    all_headers = set()
    sheet_data = []

    for f in files:
        config.logger.info(f"🔍 Reading {f['name']}")
        try:
            # Get all sheets metadata for this spreadsheet
            sheets_metadata = sheet_service.spreadsheets().get(spreadsheetId=f["id"]).execute()
            config.logger.debug(
                f"Sheets metadata for {f['name']}: {sheets_metadata.get('sheets', [])}"
            )
            for sheet in sheets_metadata.get("sheets", []):
                sheet_title = sheet.get("properties", {}).get("title")
                try:
                    values = google_api.get_sheet_values(sheet_service, f["id"], sheet_title)
                except Exception as e:
                    config.logger.error(
                        f"❌ Could not read sheet {f['name']} - sheet '{sheet_title}' – {e}"
                    )
                    continue

                if not values or len(values) < 2:
                    config.logger.warning(f"⚠️ No data in {f['name']} - sheet '{sheet_title}'")
                    continue

                header = values[0]
                rows = values[1:]
                lower_header = [h.strip().lower() for h in header]
                keep_indices = [
                    i for i, h in enumerate(lower_header) if h in config.ALLOWED_HEADERS
                ]
                if not keep_indices:
                    continue
                filtered_header = [header[i] for i in keep_indices]
                filtered_rows = [
                    [row[i] for i in keep_indices]
                    for row in rows
                    if any(cell.strip() for cell in row)
                ]
                config.logger.debug(
                    f"Filtered header for sheet '{sheet_title}': {filtered_header}, rows: {len(filtered_rows)}"
                )
                if filtered_rows:
                    all_headers.update(filtered_header)
                    sheet_data.append((filtered_header, filtered_rows))
        except Exception as e:
            config.logger.error(f"❌ Could not get sheet metadata for {f['name']} – {e}")
            continue

    if not sheet_data:
        config.logging.info(f"📭 No valid data found in folder: {year}")
        return

    final_header = list(all_headers) + ["Count"]
    final_rows = []
    for header, rows in sheet_data:
        idx_map = {h: i for i, h in enumerate(header)}
        for row in rows:
            aligned = [row[idx_map[h]] if h in idx_map else "" for h in final_header[:-1]]
            final_rows.append(aligned + [1])

    config.logger.debug(
        f"Final header for year {year}: {final_header}, total rows: {len(final_rows)}"
    )

    if "Title" in final_header:
        title_index = final_header.index("Title")
        final_rows.sort(key=lambda r: r[title_index])
    else:
        final_rows.sort()

    ss_id = google_api.create_spreadsheet(
        drive_service, name=summary_name, parent_folder_id=summary_folder_id
    )
    config.logger.debug(f"Created spreadsheet ID for {summary_name}: {ss_id}")
    config.logger.debug(f"Writing data to sheet 'Summary' with {len(final_rows)} rows")
    google_api.write_sheet_data(sheet_service, ss_id, "Summary", final_header, final_rows)
    config.logger.debug("Formatting summary sheet")
    google_api.format_summary_sheet(sheet_service, ss_id, "Summary", final_header, final_rows)

    config.logger.debug(f"Moving spreadsheet {ss_id} to folder {summary_folder_id}")
    # google_api.move_file_to_folder(drive_service, ss_id, summary_folder_id)
    config.logging.info(f"✅ Summary successfully created: {summary_name}")


def generate_complete_summary():
    """
    Generates a consolidated 'Complete Summary' spreadsheet from yearly summary spreadsheets.
    """
    try:
        drive_service = google_api.get_drive_service()
        sheets_service = google_api.get_sheets_service()
    except Exception as e:
        config.logger.error(f"Authentication failed: {e}")
        return

    # Step 0: Get or create 'Summary' folder inside parent folder DJ_SETS
    try:
        summary_folder_id = google_api.get_or_create_subfolder(
            drive_service, config.DJ_SETS, "Summary"
        )
    except HttpError as e:
        config.logger.error(f"Failed to get or create 'Summary' folder: {e}")
        return

    # List all files in summary folder
    try:
        summary_files = google_api.list_files_in_folder(drive_service, summary_folder_id)
    except HttpError as e:
        config.logger.error(f"Failed to list files in Summary folder: {e}")
        return

    # Determine output spreadsheet name
    output_name = "_pending_Complete Summary"
    # Check if any file with name containing "complete summary" exists (case insensitive)
    for f in summary_files:
        if (
            f["mimeType"] == "application/vnd.google-apps.spreadsheet"
            and "complete summary" in f["name"].lower()
        ):
            config.logger.warning(
                f'⚠️ Existing "Complete Summary" detected — using name: {output_name}'
            )
            break

    # Check if output file already exists
    existing_file = google_api.get_file_by_name(drive_service, summary_folder_id, output_name)
    if existing_file:
        master_file_id = existing_file["id"]
        config.logger.info(f'Using existing master file "{output_name}" with ID: {master_file_id}')
    else:
        # Create new spreadsheet
        master_file_id = google_api.create_spreadsheet(
            drive_service, output_name, summary_folder_id
        )

    # Open master spreadsheet info
    try:
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=master_file_id).execute()
    except HttpError as e:
        config.logger.error(f"Failed to open master spreadsheet: {e}")
        return

    # Delete all sheets except "Sheet1"
    google_api.delete_all_sheets_except(sheets_service, master_file_id, "Sheet1")

    # Clear "Sheet1"
    google_api.clear_sheet(sheets_service, master_file_id, "Sheet1")

    # Step 1: Gather and normalize all rows from summary files
    summary_data = []
    year_set = set()

    # We only want files named like "YYYY Summary"
    year_summary_pattern = re.compile(r"^(\d{4}) Summary$")

    for file in summary_files:
        file_name = file["name"].strip()
        match = year_summary_pattern.match(file_name)
        if not match:
            continue
        year = match.group(1)
        year_set.add(year)
        file_id = file["id"]

        try:
            # Get first sheet name
            file_spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=file_id).execute()
            sheets_list = file_spreadsheet.get("sheets", [])
            if not sheets_list:
                config.logger.warning(f'File "{file_name}" has no sheets, skipping.')
                continue
            source_sheet_title = sheets_list[0]["properties"]["title"]

            data = google_api.get_sheet_values(sheets_service, file_id, source_sheet_title)
            if len(data) < 2:
                continue

            headers = [str(h).strip() for h in data[0]]
            lower_headers = [h.lower() for h in headers]
            try:
                count_index = lower_headers.index("count")
            except ValueError:
                count_index = -1

            rows = data[1:]
            for row in rows:
                count = 1
                if count_index >= 0 and count_index < len(row):
                    try:
                        count = int(row[count_index])
                    except (ValueError, TypeError):
                        count = 1
                summary_data.append({"year": year, "headers": headers, "row": row, "count": count})
        except HttpError as e:
            config.logger.error(f'❌ Failed to read "{file_name}": {e}')

    if not summary_data:
        config.logger.warning("⚠️ No summary files found. Created empty Complete Summary.")
        return

    # Step 2: Build a unified header set using lowercase deduplication
    header_map = OrderedDict()  # lowercased => original casing
    for entry in summary_data:
        for h in entry["headers"]:
            key = h.lower()
            if key != "count" and key not in header_map:
                header_map[key] = h

    base_headers = list(header_map.values())
    years = sorted(year_set)
    final_headers = base_headers + years

    # Step 3: Consolidate rows using stringified deduplication keys
    row_map = dict()

    for entry in summary_data:
        year = entry["year"]
        headers = entry["headers"]
        row = entry["row"]
        count = entry["count"]

        normalized_row = {}
        for i, h in enumerate(headers):
            key = h.lower()
            if key != "count" and key in header_map:
                normalized_row[header_map[key]] = str(row[i]) if i < len(row) else ""

        signature = "|".join(normalized_row.get(h, "") for h in base_headers)

        if signature not in row_map:
            year_data = {y: "" for y in years}
            row_map[signature] = {**normalized_row, **year_data}

        existing_count = row_map[signature].get(year, "")
        try:
            existing_count_int = int(existing_count) if existing_count else 0
        except ValueError:
            existing_count_int = 0
        row_map[signature][year] = str(existing_count_int + count)

    # Step 4: Write to sheet
    final_rows = [final_headers]
    for row_obj in row_map.values():
        final_rows.append([row_obj.get(h, "") for h in final_headers])

    # Write values to sheet
    google_api.set_values(sheets_service, master_file_id, "Sheet1", 1, 1, final_rows)

    # Get sheet ID of "Sheet1"
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=master_file_id).execute()
    sheet_id = None
    for sheet in spreadsheet.get("sheets", []):
        if sheet["properties"]["title"] == "Sheet1":
            sheet_id = sheet["properties"]["sheetId"]
            break
    if sheet_id is None:
        config.logger.error('Sheet "Sheet1" not found in master spreadsheet.')
        return

    # Set header row bold
    google_api.set_bold_font(sheets_service, master_file_id, sheet_id, 1, 1, 1, len(final_headers))

    # Freeze header row
    google_api.freeze_rows(sheets_service, master_file_id, sheet_id, 1)

    # Set horizontal alignment to left for all data
    google_api.set_horizontal_alignment(
        sheets_service, master_file_id, sheet_id, 1, len(final_rows), 1, len(final_headers), "LEFT"
    )

    # Set number format to text for data rows (excluding header)
    if len(final_rows) > 1:
        google_api.set_number_format(
            sheets_service,
            master_file_id,
            sheet_id,
            2,
            len(final_rows),
            1,
            len(final_headers),
            "@STRING@",
        )

    # Auto resize columns and adjust width with max 200 pixels
    # Google Sheets API does not support setting column width directly with batchUpdate.
    # We can only auto resize columns.
    google_api.auto_resize_columns(sheets_service, master_file_id, sheet_id, 1, len(final_headers))

    config.logger.info(f"✅ {output_name} generated.")


def main():
    """
    Main entry point for module execution.
    """
    generate_next_missing_summary()
    generate_complete_summary()


if __name__ == "__main__":
    main()
