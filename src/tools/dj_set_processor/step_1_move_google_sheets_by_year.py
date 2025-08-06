import logging
import re
import csv
import io
import time

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Constants for folder IDs (to be set appropriately)
CSV_FILES = "YOUR_CSV_FILES_FOLDER_ID"
DJ_SETS = "YOUR_DJ_SETS_FOLDER_ID"

# MIME types
MIME_GOOGLE_SHEETS = "application/vnd.google-apps.spreadsheet"
MIME_CSV = "text/csv"
MIME_PLAIN_TEXT = "text/plain"

# Initialize the Drive API client
drive_service = build("drive", "v3")


def try_lock_folder(year):
    # Placeholder for locking mechanism
    # Return True if lock acquired, False otherwise
    return True


def release_folder_lock(year):
    # Placeholder for releasing lock
    pass


def get_or_create_subfolder(parent_id, name):
    # Check if subfolder exists
    query = f"mimeType = 'application/vnd.google-apps.folder' and name = '{name}' and '{parent_id}' in parents and trashed = false"
    response = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = response.get("files", [])
    if files:
        return files[0]["id"]
    # Create folder
    file_metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = drive_service.files().create(body=file_metadata, fields="id").execute()
    return folder["id"]


def list_files_in_folder(folder_id):
    files = []
    page_token = None
    while True:
        response = (
            drive_service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed = false",
                spaces="drive",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
            )
            .execute()
        )
        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken", None)
        if page_token is None:
            break
    return files


def move_file_to_folder(file_id, old_parent_id, new_parent_id):
    # Remove old parent and add new parent
    try:
        drive_service.files().update(
            fileId=file_id,
            addParents=new_parent_id,
            removeParents=old_parent_id,
            fields="id, parents",
        ).execute()
    except HttpError as e:
        logging.debug(f"Error moving file {file_id}: {e}")
        raise


def rename_file(file_id, new_name):
    try:
        drive_service.files().update(fileId=file_id, body={"name": new_name}).execute()
    except HttpError as e:
        logging.debug(f"Error renaming file {file_id}: {e}")
        raise


def create_placeholder_file(folder_id, name):
    file_metadata = {"name": name, "parents": [folder_id], "mimeType": MIME_CSV}
    # Create empty CSV file
    media = None
    try:
        drive_service.files().create(body=file_metadata, fields="id").execute()
    except HttpError as e:
        logging.debug(f"Error creating placeholder file {name}: {e}")
        raise


def get_file_content(file_id):
    try:
        request = drive_service.files().export_media(fileId=file_id, mimeType=MIME_CSV)
        fh = io.BytesIO()
        downloader = googleapiclient.http.MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read().decode("utf-8")
    except HttpError as e:
        logging.debug(f"Error downloading file content {file_id}: {e}")
        raise


def create_file_in_folder(folder_id, name, content, mime_type=MIME_CSV):
    media_body = googleapiclient.http.MediaInMemoryUpload(
        content.encode("utf-8"), mimetype=mime_type
    )
    file_metadata = {"name": name, "parents": [folder_id]}
    try:
        drive_service.files().create(
            body=file_metadata, media_body=media_body, fields="id"
        ).execute()
    except HttpError as e:
        logging.debug(f"Error creating file {name} in folder {folder_id}: {e}")
        raise


def trash_file(file_id):
    try:
        drive_service.files().update(fileId=file_id, body={"trashed": True}).execute()
    except HttpError as e:
        logging.debug(f"Error trashing file {file_id}: {e}")
        raise


def move_google_sheets_by_year():
    start_time = time.time()
    MAX_RUNTIME = 5 * 60  # seconds
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}")

    files_by_year = {}
    all_files = list_files_in_folder(CSV_FILES)

    for file in all_files:
        name = file["name"].strip()
        mime = file["mimeType"]
        if (
            mime == MIME_GOOGLE_SHEETS
            and date_pattern.match(name)
            and not name.startswith("FAILED_")
            and not name.startswith("possible_duplicate_")
            and not name.startswith("_DUPLICATE_LOG")
        ):
            year = name[0:4]
            files_by_year.setdefault(year, []).append(file)

    locked_year = None
    for year in files_by_year.keys():
        if try_lock_folder(year):
            locked_year = year
            break

    if not locked_year:
        logging.debug("🔒 No available year lock. Exiting.")
        return

    logging.debug(f"🔐 Locked year: {locked_year}")
    processed = 0

    try:
        year_folder_id = get_or_create_subfolder(DJ_SETS, locked_year)
        archive_folder_id = get_or_create_subfolder(year_folder_id, "Archive")

        current_files = list_files_in_folder(CSV_FILES)
        archive_files = list_files_in_folder(archive_folder_id)
        archive_file_names = set(f["name"] for f in archive_files)

        for file in current_files:
            if time.time() - start_time > MAX_RUNTIME:
                break

            name = file["name"].strip()
            mime = file["mimeType"]
            if (
                mime == MIME_GOOGLE_SHEETS
                and name.startswith(locked_year)
                and not name.startswith("FAILED_")
                and not name.startswith("possible_duplicate_")
                and not name.startswith("_DUPLICATE_LOG")
            ):
                placeholder_name = f"{name}_original_google_sheet.csv"

                if placeholder_name in archive_file_names:
                    dup_name = f"possible_duplicate_{name}"
                    rename_file(file["id"], dup_name)
                    logging.debug(f'⚠️ Duplicate found in archive. Renamed to "{dup_name}"')
                    continue

                move_file_to_folder(file["id"], CSV_FILES, year_folder_id)
                create_placeholder_file(archive_folder_id, placeholder_name)
                logging.debug(f'✅ Moved "{name}" to "{locked_year}/", placeholder created')
                processed += 1

    except Exception as e:
        logging.debug(f"❌ Error processing files for {locked_year}: {e}")
    finally:
        release_folder_lock(locked_year)

    logging.debug(f"🏁 Google Sheets run complete for {locked_year}. Files processed: {processed}")


def move_csv_files_by_year():
    start_time = time.time()
    MAX_RUNTIME = 5 * 60  # seconds
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}")

    all_files = list_files_in_folder(CSV_FILES)
    target_files = []

    for file in all_files:
        name = file["name"].strip()
        mime = file["mimeType"]
        if (
            (mime == MIME_CSV or mime == MIME_PLAIN_TEXT)
            and date_pattern.match(name)
            and not name.startswith("Copy of ")
            and not name.startswith("possible_duplicate_")
            and not name.startswith("FAILED_")
            and not name.startswith("_DUPLICATE_LOG")
        ):
            target_files.append(file)

    processed = 0

    for file in target_files:
        if time.time() - start_time > MAX_RUNTIME:
            logging.debug(f"⏸️ Time limit reached. Processed {processed} file(s).")
            break

        original_name = file["name"].strip()
        match = date_pattern.match(original_name)
        year = original_name[0:4]

        if not try_lock_folder(year):
            logging.debug(f'🔒 Year "{year}" is locked — skipping "{original_name}"')
            continue

        try:
            year_folder_id = get_or_create_subfolder(DJ_SETS, year)
            archive_folder_id = get_or_create_subfolder(year_folder_id, "Archive")

            archive_files = list_files_in_folder(archive_folder_id)
            is_duplicate = any(f["name"].startswith(original_name) for f in archive_files)

            if is_duplicate:
                duplicate_name = f"possible_duplicate_{original_name}"
                rename_file(file["id"], duplicate_name)
                logging.debug(f'⚠️ Duplicate found. Renamed to "{duplicate_name}"')
                continue

            # Archive original by making a copy
            drive_service.files().copy(
                fileId=file["id"], body={"name": original_name, "parents": [archive_folder_id]}
            ).execute()
            logging.debug(f'📦 Archived original: "{original_name}"')

            # Download file content as CSV text
            request = drive_service.files().get_media(fileId=file["id"])
            fh = io.BytesIO()
            downloader = googleapiclient.http.MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            fh.seek(0)
            blob = fh.read().decode("utf-8")

            lines = blob.replace("\r\n", "\n").split("\n")
            if lines and re.match(r"(?i)^sep\s*=", lines[0]):
                lines = lines[1:]

            # Parse CSV and clean
            cleaned_rows = []
            for line in lines:
                if line.strip() == "":
                    continue

                # Protect quoted commas by replacing commas inside quotes with underscores
                def replace_commas_in_quotes(match):
                    return '"' + match.group(1).replace(",", "_") + '"'

                protected_line = re.sub(r'"([^"]*?)"', replace_commas_in_quotes, line)
                # Split by commas
                cells = [
                    cell.strip().strip('"').replace('"', "'") for cell in protected_line.split(",")
                ]
                cleaned_rows.append(cells)

            if not cleaned_rows or not cleaned_rows[0]:
                raise Exception("No valid rows")

            output_io = io.StringIO()
            writer = csv.writer(output_io, quoting=csv.QUOTE_ALL)
            for row in cleaned_rows:
                writer.writerow(row)
            output = output_io.getvalue()

            cleaned_name = re.sub(r"\.csv$", "_Cleaned.csv", original_name, flags=re.IGNORECASE)
            create_file_in_folder(year_folder_id, cleaned_name, output, MIME_CSV)
            trash_file(file["id"])

            logging.debug(f'✅ Cleaned + moved: "{cleaned_name}"')
            processed += 1

        except Exception as e:
            failed_name = f"FAILED_{file['name'].strip()}"
            try:
                rename_file(file["id"], failed_name)
            except Exception:
                pass
            logging.debug(f"❌ Cleaning failed for \"{file['name']}\": {e}")
        finally:
            release_folder_lock(year)

    logging.debug(f"🏁 CSV run complete. Total processed: {processed}")
