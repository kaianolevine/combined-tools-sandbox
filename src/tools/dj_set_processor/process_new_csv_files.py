import os

import config
import core.google_drive as google_api
from core import logger as log
import tools.dj_set_processor.helpers as helpers

log = log.get_logger()


# --- Utility: remove summary file for a given year ---
def remove_summary_file_for_year(drive, year):
    try:
        summary_folder_id = google_api.get_or_create_folder(
            config.DJ_SETS_FOLDER_ID, "Summary", drive
        )
        summary_query = (
            f"name = '{year} Summary' and '{summary_folder_id}' in parents and trashed = false"
        )
        summary_resp = (
            drive.files()
            .list(
                q=summary_query,
                spaces="drive",
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        summary_files = summary_resp.get("files", [])
        for summary_file in summary_files:
            drive.files().delete(fileId=summary_file["id"], supportsAllDrives=True).execute()
            log.info(
                f"🗑️ Deleted existing summary file '{summary_file.get('name')}' for year {year}"
            )
    except Exception as e:
        log.error(f"Failed to remove summary file for year {year}: {e}")


# --- Utility: check for duplicate base filename in a folder ---
def file_exists_with_base_name(drive, folder_id, base_name):
    try:
        resp = (
            drive.files()
            .list(
                q=f"'{folder_id}' in parents and trashed = false",
                spaces="drive",
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        candidates = resp.get("files", [])
        for f in candidates:
            if os.path.splitext(f.get("name", ""))[0] == base_name:
                return True
    except Exception as e:
        log.error(f"Error checking for duplicates in folder {folder_id}: {e}")
    return False


# === MAIN ===
def main():
    log.info("Starting main process")
    drive = google_api.get_drive_service()

    # Normalize any leftover status prefixes before processing
    helpers.normalize_prefixes_in_source(drive)

    files = google_api.list_files_in_folder(drive, config.CSV_SOURCE_FOLDER_ID)
    log.info(f"Found {len(files)} files in source folder")
    for file_metadata in files:
        filename = file_metadata["name"]
        log.debug(f"Processing file: {filename}")
        file_id = file_metadata["id"]

        year = helpers.extract_year_from_filename(filename)
        if not year:
            log.warning(f"⚠️ Skipping unrecognized filename format: {filename}")
            continue

        # If the file is not a CSV but starts with a year, move it straight to the year folder
        if not filename.lower().endswith(".csv"):
            log.info(f"\n📄 Moving non-CSV file that starts with year: {filename}")
            try:
                year_folder_id = google_api.get_or_create_folder(
                    config.DJ_SETS_FOLDER_ID, year, drive
                )

                # Check for duplicate base name in destination
                base_name = os.path.splitext(filename)[0]
                if file_exists_with_base_name(drive, year_folder_id, base_name):
                    log.warning(
                        f"⚠️ Destination already contains file with base name '{base_name}' in year folder {year_folder_id}. Marking original as possible duplicate and skipping."
                    )
                    try:
                        new_name = f"possible_duplicate_{filename}"
                        drive.files().update(
                            fileId=file_id, body={"name": new_name}, supportsAllDrives=True
                        ).execute()
                        log.info(f"✏️ Renamed original to '{new_name}'")
                    except Exception as rename_exc:
                        log.error(
                            f"Failed to rename original to possible_duplicate_: {rename_exc}"
                        )
                    continue

                try:
                    drive.files().update(
                        fileId=file_id,
                        addParents=year_folder_id,
                        removeParents=config.CSV_SOURCE_FOLDER_ID,
                        supportsAllDrives=True,
                    ).execute()
                    log.info(f"📦 Moved original file to {year} subfolder: {filename}")
                except Exception as move_exc:
                    log.error(f"Failed to move original file to {year} subfolder: {move_exc}")
                remove_summary_file_for_year(drive, year)

            except Exception as e:
                log.error(f"Failed to move non-CSV file {filename}: {e}")

            continue

        # At this point we only process CSVs
        log.info(f"\n🚧 Processing: {filename}")
        temp_path = os.path.join("/tmp", filename)
        google_api.download_file(drive, file_id, temp_path)
        helpers.normalize_csv(temp_path)
        log.info(f"Downloaded and normalized file: {filename}")

        # Get or create year folder in Drive
        year_folder_id = google_api.get_or_create_folder(config.DJ_SETS_FOLDER_ID, year, drive)

        # BEFORE processing: check if destination already contains a file with the same name.
        base_name = os.path.splitext(filename)[0]
        log.debug(
            f"Checking destination year folder {year_folder_id} for existing files matching base name '{base_name}' (ignoring extensions)"
        )
        if file_exists_with_base_name(drive, year_folder_id, base_name):
            log.warning(
                f"⚠️ Destination already contains file with base name '{base_name}' in year folder {year_folder_id}. Marking original as possible duplicate and skipping."
            )
            try:
                new_name = f"possible_duplicate_{filename}"
                drive.files().update(
                    fileId=file_id, body={"name": new_name}, supportsAllDrives=True
                ).execute()
                log.info(f"✏️ Renamed original to '{new_name}'")
            except Exception as rename_exc:
                log.error(f"Failed to rename original to possible_duplicate_: {rename_exc}")
            # cleanup temp and skip
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
            continue

        # Upload cleaned CSV as Google Sheet
        try:
            sheet_id = google_api.upload_to_drive(drive, temp_path, year_folder_id)
            log.debug(f"Uploaded sheet ID: {sheet_id}")

            # Apply formatting only if sheet was populated with valid data
            google_api.apply_formatting_to_sheet(sheet_id)

            # Delete existing summary file in Summary folder under top-level DJ_SETS_FOLDER_ID
            remove_summary_file_for_year(drive, year)

            # Move original file to Archive subfolder instead of deleting
            try:
                archive_folder_id = google_api.get_or_create_folder(
                    year_folder_id, "Archive", drive
                )
                drive.files().update(
                    fileId=file_id,
                    addParents=archive_folder_id,
                    removeParents=config.CSV_SOURCE_FOLDER_ID,
                    supportsAllDrives=True,
                ).execute()
                log.info(f"📦 Moved original file to Archive subfolder: {filename}")
            except Exception as move_exc:
                log.error(f"Failed to move original file to Archive subfolder: {move_exc}")

            # drive.files().delete(fileId=file_id, supportsAllDrives=True).execute()
            # log.info(f"🗑️ Deleted original file from Drive: {filename}")

        except Exception as e:
            log.error(f"❌ Failed to upload or format {filename}: {e}")
            # Mark original as failed
            try:
                failed_name = f"FAILED_{filename}"
                drive.files().update(
                    fileId=file_id, body={"name": failed_name}, supportsAllDrives=True
                ).execute()
                log.info(f"✏️ Renamed original to '{failed_name}'")
            except Exception as rename_exc:
                log.error(f"Failed to rename original to FAILED_: {rename_exc}")
        finally:
            # cleanup temp file
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

        log.info(f"\n🚧 Processing: {filename}")
        temp_path = os.path.join("/tmp", filename)
        google_api.download_file(drive, file_id, temp_path)
        helpers.normalize_csv(temp_path)
        log.info(f"Downloaded and normalized file: {filename}")

        # Get or create year folder in Drive
        year_folder_id = google_api.get_or_create_folder(config.DJ_SETS_FOLDER_ID, year, drive)

        # BEFORE processing: check if destination already contains a file with the same name.
        base_name = os.path.splitext(filename)[0]
        log.debug(
            f"Checking destination year folder {year_folder_id} for existing files matching base name '{base_name}' (ignoring extensions)"
        )
        if file_exists_with_base_name(drive, year_folder_id, base_name):
            log.warning(
                f"⚠️ Destination already contains file with base name '{base_name}' in year folder {year_folder_id}. Marking original as possible duplicate and skipping."
            )
            try:
                new_name = f"possible_duplicate_{filename}"
                drive.files().update(
                    fileId=file_id, body={"name": new_name}, supportsAllDrives=True
                ).execute()
                log.info(f"✏️ Renamed original to '{new_name}'")
            except Exception as rename_exc:
                log.error(f"Failed to rename original to possible_duplicate_: {rename_exc}")
            # cleanup temp and skip
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
            continue

        # Upload cleaned CSV as Google Sheet
        try:
            sheet_id = google_api.upload_to_drive(drive, temp_path, year_folder_id)
            log.debug(f"Uploaded sheet ID: {sheet_id}")

            # Apply formatting only if sheet was populated with valid data
            google_api.apply_formatting_to_sheet(sheet_id)

            # Delete existing summary file in Summary folder under top-level DJ_SETS_FOLDER_ID
            remove_summary_file_for_year(drive, year)

            # Move original file to Archive subfolder instead of deleting
            try:
                archive_folder_id = google_api.get_or_create_folder(
                    year_folder_id, "Archive", drive
                )
                drive.files().update(
                    fileId=file_id,
                    addParents=archive_folder_id,
                    removeParents=config.CSV_SOURCE_FOLDER_ID,
                    supportsAllDrives=True,
                ).execute()
                log.info(f"📦 Moved original file to Archive subfolder: {filename}")
            except Exception as move_exc:
                log.error(f"Failed to move original file to Archive subfolder: {move_exc}")

            # drive.files().delete(fileId=file_id, supportsAllDrives=True).execute()
            # log.info(f"🗑️ Deleted original file from Drive: {filename}")

        except Exception as e:
            log.error(f"❌ Failed to upload or format {filename}: {e}")
            # Mark original as failed
            try:
                failed_name = f"FAILED_{filename}"
                drive.files().update(
                    fileId=file_id, body={"name": failed_name}, supportsAllDrives=True
                ).execute()
                log.info(f"✏️ Renamed original to '{failed_name}'")
            except Exception as rename_exc:
                log.error(f"Failed to rename original to FAILED_: {rename_exc}")
        finally:
            # cleanup temp file
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass


if __name__ == "__main__":
    main()
