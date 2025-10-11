import os
import re

import config
import core.google_drive as google_api
from core import logger as log

log = log.get_logger()


def extract_year_from_filename(filename):
    log.debug(f"extract_year_from_filename called with filename: {filename}")
    match = re.match(r"(\d{4})[-_]", filename)
    year = match.group(1) if match else None
    log.debug(f"Extracted year: {year} from filename: {filename}")
    return year


def normalize_csv(file_path):
    log.debug(f"normalize_csv called with file_path: {file_path} - reading file")
    with open(file_path, "r") as f:
        lines = f.readlines()
    cleaned_lines = [re.sub(r"\s+", " ", line).strip() for line in lines if line.strip()]
    log.debug(f"Lines after cleaning: {len(cleaned_lines)}")
    with open(file_path, "w") as f:
        f.write("\n".join(cleaned_lines))
    log.info(f"✅ Normalized: {file_path}")


def normalize_prefixes_in_source(drive):
    """Remove leading status prefixes from files in the CSV source folder.
    If a file name starts with 'FAILED_' or 'possible_duplicate_' (case-insensitive),
    this function will attempt to rename it to the original base name (i.e. strip the prefix).
    Uses supportsAllDrives=True to operate on shared drives.
    """
    try:
        log.debug("normalize_prefixes_in_source: listing source folder files")
        resp = (
            drive.files()
            .list(
                q=f"'{config.CSV_SOURCE_FOLDER_ID}' in parents and trashed = false",
                spaces="drive",
                fields="nextPageToken, files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files = resp.get("files", [])
        log.info(f"normalize_prefixes_in_source: found {len(files)} files to inspect")

        for f in files:
            original_name = f.get("name", "")
            lower = original_name.lower()
            prefix = None
            if lower.startswith("failed_"):
                prefix = original_name[:7]
            elif lower.startswith("possible_duplicate_"):
                prefix = original_name[:19]
            elif lower.startswith("copy of "):
                prefix = original_name[:8]

            if prefix:
                new_name = original_name[len(prefix) :]
                # If new_name is empty or already exists, skip
                if not new_name:
                    log.warning(
                        f"normalize_prefixes_in_source: derived empty new name for {original_name}, skipping"
                    )
                    continue

                # Check if a file with target name already exists in the same folder
                try:
                    query = f"name = '{new_name}' and '{config.CSV_SOURCE_FOLDER_ID}' in parents and trashed = false"
                    exists_resp = (
                        drive.files()
                        .list(
                            q=query,
                            fields="files(id, name)",
                            supportsAllDrives=True,
                            includeItemsFromAllDrives=True,
                        )
                        .execute()
                    )
                    if exists_resp.get("files"):
                        log.info(
                            f"normalize_prefixes_in_source: target name '{new_name}' already exists in source folder — leaving '{original_name}' as-is"
                        )
                        continue
                except Exception as e:
                    log.debug(
                        f"normalize_prefixes_in_source: error checking existing file for {new_name}: {e}"
                    )

                try:
                    log.info(
                        f"normalize_prefixes_in_source: renaming '{original_name}' -> '{new_name}'"
                    )
                    drive.files().update(
                        fileId=f["id"], body={"name": new_name}, supportsAllDrives=True
                    ).execute()
                except Exception as e:
                    log.error(
                        f"normalize_prefixes_in_source: failed to rename {original_name}: {e}"
                    )
    except Exception as e:
        log.error(f"normalize_prefixes_in_source: unexpected error: {e}")


# === MAIN ===
def main():
    log.info("Starting main process")
    drive = google_api.get_drive_service()

    # Normalize any leftover status prefixes before processing
    normalize_prefixes_in_source(drive)

    files = google_api.list_files_in_folder(drive, config.CSV_SOURCE_FOLDER_ID)
    log.info(f"Found {len(files)} files in source folder")
    for file_metadata in files:
        filename = file_metadata["name"]
        log.debug(f"Processing file: {filename}")
        file_id = file_metadata["id"]

        year = extract_year_from_filename(filename)
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
                dup_resp = (
                    drive.files()
                    .list(
                        q=f"'{year_folder_id}' in parents and trashed = false",
                        spaces="drive",
                        fields="nextPageToken, files(id, name)",
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                    )
                    .execute()
                )
                dup_candidates = dup_resp.get("files", [])
                dup_files = [
                    f
                    for f in dup_candidates
                    if os.path.splitext(f.get("name", ""))[0] == base_name
                ]

                if dup_files:
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

            except Exception as e:
                log.error(f"Failed to move non-CSV file {filename}: {e}")

            continue

        # At this point we only process CSVs
        log.info(f"\n🚧 Processing: {filename}")
        temp_path = os.path.join("/tmp", filename)
        google_api.download_file(drive, file_id, temp_path)
        normalize_csv(temp_path)
        log.info(f"Downloaded and normalized file: {filename}")

        # Get or create year folder in Drive
        year_folder_id = google_api.get_or_create_folder(config.DJ_SETS_FOLDER_ID, year, drive)

        # BEFORE processing: check if destination already contains a file with the same name.

        try:
            base_name = os.path.splitext(filename)[0]
            log.debug(
                f"Checking destination year folder {year_folder_id} for existing files matching base name '{base_name}' (ignoring extensions)"
            )
            dup_resp = (
                drive.files()
                .list(
                    q=f"'{year_folder_id}' in parents and trashed = false",
                    spaces="drive",
                    fields="nextPageToken, files(id, name)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )
            dup_candidates = dup_resp.get("files", [])
            dup_files = [
                f for f in dup_candidates if os.path.splitext(f.get("name", ""))[0] == base_name
            ]
            if dup_files:
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
        except Exception as e:
            log.error(f"Error while checking for duplicates in destination folder: {e}")
            # proceed — we'll try processing but be cautious

        # Upload cleaned CSV as Google Sheet
        try:
            sheet_id = google_api.upload_to_drive(drive, temp_path, year_folder_id)
            log.debug(f"Uploaded sheet ID: {sheet_id}")

            # Apply formatting only if sheet was populated with valid data
            google_api.apply_formatting_to_sheet(sheet_id)

            # Delete existing summary file in Summary folder under top-level DJ_SETS_FOLDER_ID
            try:
                summary_folder_id = google_api.get_or_create_folder(
                    config.DJ_SETS_FOLDER_ID, "Summary", drive
                )
                summary_query = f"name = '{year} Summary' and '{summary_folder_id}' in parents and trashed = false"
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
                    drive.files().delete(
                        fileId=summary_file["id"], supportsAllDrives=True
                    ).execute()
                    log.info(
                        f"Deleted existing summary file '{summary_file.get('name')}' in Summary folder {summary_folder_id}"
                    )
            except Exception as summary_exc:
                log.error(f"Failed to check/delete existing summary file: {summary_exc}")

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
        normalize_csv(temp_path)
        log.info(f"Downloaded and normalized file: {filename}")

        # Get or create year folder in Drive
        year_folder_id = google_api.get_or_create_folder(config.DJ_SETS_FOLDER_ID, year, drive)

        # BEFORE processing: check if destination already contains a file with the same name.

        try:
            base_name = os.path.splitext(filename)[0]
            log.debug(
                f"Checking destination year folder {year_folder_id} for existing files matching base name '{base_name}' (ignoring extensions)"
            )
            dup_resp = (
                drive.files()
                .list(
                    q=f"'{year_folder_id}' in parents and trashed = false",
                    spaces="drive",
                    fields="nextPageToken, files(id, name)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )
            dup_candidates = dup_resp.get("files", [])
            dup_files = [
                f for f in dup_candidates if os.path.splitext(f.get("name", ""))[0] == base_name
            ]
            if dup_files:
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
        except Exception as e:
            log.error(f"Error while checking for duplicates in destination folder: {e}")
            # proceed — we'll try processing but be cautious

        # Upload cleaned CSV as Google Sheet
        try:
            sheet_id = google_api.upload_to_drive(drive, temp_path, year_folder_id)
            log.debug(f"Uploaded sheet ID: {sheet_id}")

            # Apply formatting only if sheet was populated with valid data
            google_api.apply_formatting_to_sheet(sheet_id)

            # Delete existing summary file in Summary folder under top-level DJ_SETS_FOLDER_ID
            try:
                summary_folder_id = google_api.get_or_create_folder(
                    config.DJ_SETS_FOLDER_ID, "Summary", drive
                )
                summary_query = f"name = '{year} Summary' and '{summary_folder_id}' in parents and trashed = false"
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
                    drive.files().delete(
                        fileId=summary_file["id"], supportsAllDrives=True
                    ).execute()
                    log.info(
                        f"Deleted existing summary file '{summary_file.get('name')}' in Summary folder {summary_folder_id}"
                    )
            except Exception as summary_exc:
                log.error(f"Failed to check/delete existing summary file: {summary_exc}")

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
