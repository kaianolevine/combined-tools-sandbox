import core.google_drive as google_drive
import core.google_sheets as google_sheets
import core.sheets_formatting as format
import core.logger as log
import config
import tools.dj_set_processor.deduplication as deduplication

log = log.get_logger()


def generate_next_missing_summary():
    """
    Generate the next missing summary for a year, if not locked.
    """
    log.info("🚀 Starting generate_next_missing_summary()")
    drive_service = google_drive.get_drive_service()
    sheet_service = google_sheets.get_sheets_service()

    summary_folder = google_drive.get_or_create_folder(
        config.DJ_SETS_FOLDER_ID, config.SUMMARY_FOLDER_NAME, drive_service
    )
    log.debug(f"Summary folder: {summary_folder}")

    # if not helpers.try_lock_folder(config.SUMMARY_FOLDER_NAME):
    #    log.info("🔒 Summary generation is locked. Skipping run.")
    #    return

    year_folders = google_drive.get_files_in_folder(
        drive_service, config.DJ_SETS_FOLDER_ID, mime_type="application/vnd.google-apps.folder"
    )
    log.debug(f"Year folders found: {[f['name'] for f in year_folders]}")
    for folder in year_folders:
        year = folder["name"]
        if year.lower() == "summary":
            continue

        summary_name = f"{year} Summary"
        existing_summaries = google_drive.get_files_in_folder(
            drive_service, summary_folder, name_contains=summary_name
        )
        log.debug(f"Found existing summaries for {year}: {existing_summaries}")
        if existing_summaries:
            log.info(f"✅ Summary already exists for {year}")
            continue

        log.debug(f"Getting files for year {year}")
        files = google_drive.get_files_in_folder(
            drive_service, folder["id"], mime_type="application/vnd.google-apps.spreadsheet"
        )
        if any(f["name"].startswith("FAILED_") or "_Cleaned" in f["name"] for f in files):
            log.info(f"⛔ Skipping year {year} — unready files found")
            continue

        # if not helpers.try_lock_folder(year):
        #    log.info(f"🔒 Year {year} is locked. Skipping.")
        #    continue

        # try:
        log.debug(f"Files to process for {year}: {[f['name'] for f in files]}")
        log.info(f"🔧 Generating summary for {year}...")
        generate_summary_for_folder(
            drive_service, sheet_service, files, summary_folder, summary_name, year
        )
        # finally:
        #    helpers.release_folder_lock(year)
        #    helpers.release_folder_lock(config.SUMMARY_FOLDER_NAME)


def generate_summary_for_folder(
    drive_service, sheet_service, files, summary_folder_id, summary_name, year
):
    log.debug(f"Starting generate_summary_for_folder for year {year} with {len(files)} files")
    all_headers = set()
    sheet_data = []

    for f in files:
        log.info(f"🔍 Reading {f['name']}")
        try:
            # Get all sheets metadata for this spreadsheet
            sheets_metadata = sheet_service.spreadsheets().get(spreadsheetId=f["id"]).execute()
            log.debug(f"Sheets metadata for {f['name']}: {sheets_metadata.get('sheets', [])}")
            for sheet in sheets_metadata.get("sheets", []):
                sheet_title = sheet.get("properties", {}).get("title")
                try:
                    values = google_sheets.get_sheet_values(sheet_service, f["id"], sheet_title)
                except Exception as e:
                    log.error(f"❌ Could not read sheet {f['name']} - sheet '{sheet_title}' – {e}")
                    continue

                if not values or len(values) < 2:
                    log.warning(f"⚠️ No data in {f['name']} - sheet '{sheet_title}'")
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
                log.debug(
                    f"Filtered header for sheet '{sheet_title}': {filtered_header}, rows: {len(filtered_rows)}"
                )
                if filtered_rows:
                    all_headers.update(filtered_header)
                    sheet_data.append((filtered_header, filtered_rows))
        except Exception as e:
            log.error(f"❌ Could not get sheet metadata for {f['name']} – {e}")
            continue

    if not sheet_data:
        log.info(f"📭 No valid data found in folder: {year}")
        return

    ordered_header = [col for col in config.desiredOrder if col in all_headers]
    unordered_header = [col for col in all_headers if col not in config.desiredOrder]
    final_header = ordered_header + unordered_header + ["Count"]
    final_rows = []
    for header, rows in sheet_data:
        idx_map = {h: i for i, h in enumerate(header)}
        for row in rows:
            aligned = [row[idx_map[h]] if h in idx_map else "" for h in final_header[:-1]]
            final_rows.append(aligned + [1])

    log.debug(f"Final header for year {year}: {final_header}, total rows: {len(final_rows)}")

    if "Title" in final_header:
        title_index = final_header.index("Title")
        final_rows.sort(key=lambda r: r[title_index])
    else:
        final_rows.sort()

    ss_id = google_drive.create_spreadsheet(
        drive_service, name=summary_name, parent_folder_id=summary_folder_id
    )
    log.debug(f"Created spreadsheet ID for {summary_name}: {ss_id}")

    # Ensure a sheet named "Summary" exists
    spreadsheet_info = sheet_service.spreadsheets().get(spreadsheetId=ss_id).execute()
    sheets = spreadsheet_info.get("sheets", [])
    found_summary = False
    for sheet in sheets:
        if sheet.get("properties", {}).get("title") == "Summary":
            found_summary = True
            break
    if not found_summary and sheets:
        # Rename the first sheet to "Summary"
        first_sheet_id = sheets[0]["properties"]["sheetId"]
        google_sheets.rename_sheet(sheet_service, ss_id, first_sheet_id, "Summary")

    # Delete all sheets except "Summary"
    log.info(f"Deleting all sheets except 'Summary' in spreadsheet {ss_id}")
    google_sheets.delete_all_sheets_except(sheet_service, ss_id, "Summary")
    log.debug("All sheets except 'Summary' deleted.")

    # Write summary data to "Summary" sheet
    log.info(f"Writing summary data to 'Summary' sheet with {len(final_rows)} rows")
    google_sheets.write_sheet_data(sheet_service, ss_id, "Summary", final_header, final_rows)
    log.debug("Summary data written to 'Summary' sheet.")

    # Format the "Summary" sheet
    log.info("Formatting 'Summary' sheet")
    # Get sheet ID for "Summary"
    spreadsheet = sheet_service.spreadsheets().get(spreadsheetId=ss_id).execute()
    summary_sheet_id = None
    for sheet in spreadsheet.get("sheets", []):
        if sheet["properties"]["title"] == "Summary":
            summary_sheet_id = sheet["properties"]["sheetId"]
            break
    if summary_sheet_id is None:
        log.error('Sheet "Summary" not found in spreadsheet.')
        return
    # Set all cells (including header) to plain text format
    format.set_number_format(
        sheet_service,
        ss_id,
        summary_sheet_id,
        1,
        len(final_rows) + 1,
        1,
        len(final_header),
        "@STRING@",
    )
    # Set header row bold
    format.set_bold_font(sheet_service, ss_id, summary_sheet_id, 1, 1, 1, len(final_header))
    # Freeze header row
    format.freeze_rows(sheet_service, ss_id, summary_sheet_id, 1)
    # Set horizontal alignment to left for all data
    format.set_horizontal_alignment(
        sheet_service,
        ss_id,
        summary_sheet_id,
        1,
        len(final_rows) + 1,
        1,
        len(final_header),
        "LEFT",
    )
    # Auto resize columns and adjust width with max 200 pixels
    format.auto_resize_columns(sheet_service, ss_id, summary_sheet_id, 1, len(final_header))
    log.info("Formatting of 'Summary' sheet complete.")

    # log.debug(f"Moving spreadsheet {ss_id} to folder {summary_folder_id}")
    # google_api.move_file_to_folder(drive_service, ss_id, summary_folder_id)
    # log.info(f"✅ Summary successfully created: {summary_name}")

    log.info(f"Starting deduplication for: {summary_name}")
    deduplication.deduplicate_summary(ss_id)
    log.info(f"✅ Deduplication completed for: {summary_name}")


if __name__ == "__main__":
    generate_next_missing_summary()
