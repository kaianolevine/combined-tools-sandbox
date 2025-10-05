import os
import re
import logging
from typing import Optional

from googleapiclient.discovery import build
from google.oauth2 import service_account

from tools.dj_set_processor.step_0_normalize import normalize_google_sheet

# Constants
DJ_SETS = os.getenv("DJ_SETS", "1A0tKQ2DBXI1Bt9h--olFwnBNne3am-rL")  # Default or set via env var
MIME_GOOGLE_SHEETS = "application/vnd.google-apps.spreadsheet"

SCOPES = ["https://www.googleapis.com/auth/drive"]


def extract_year_from_filename(name: str) -> Optional[str]:
    """
    Extracts the year from a filename if it starts with YYYY-MM-DD.
    Returns the year as a string or None if pattern not matched.
    """
    match = re.match(r"^(\d{4})-\d{2}-\d{2}", name)
    if match:
        return match.group(1)
    return None


def get_drive_service():
    """
    Builds and returns the Google Drive API service using service account credentials.
    """
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=SCOPES
    )
    return build("drive", "v3", credentials=credentials)


def get_or_create_year_folder(drive_service, parent_folder_id: str, year: str) -> str:
    """
    Finds or creates a folder named 'year' under the parent_folder_id.
    Returns the folder ID.
    """
    query = (
        f"mimeType = 'application/vnd.google-apps.folder' "
        f"and name = '{year}' "
        f"and '{parent_folder_id}' in parents "
        f"and trashed = false"
    )
    response = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = response.get("files", [])
    if files:
        return files[0]["id"]
    # Create folder
    folder_metadata = {
        "name": year,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_folder_id],
    }
    folder = drive_service.files().create(body=folder_metadata, fields="id").execute()
    return folder["id"]


def list_google_sheets_in_folder(drive_service, folder_id: str):
    """
    Lists all Google Sheets files in the specified folder.
    Returns a list of file dicts with at least 'id' and 'name'.
    """
    files = []
    page_token = None
    query = f"mimeType = '{MIME_GOOGLE_SHEETS}' and '{folder_id}' in parents and trashed = false"
    while True:
        response = (
            drive_service.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name)",
                pageToken=page_token,
            )
            .execute()
        )
        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken", None)
        if not page_token:
            break
    return files


def copy_file_to_folder(
    drive_service, file_id: str, new_parent_id: str, new_name: str
) -> Optional[str]:
    """
    Copies a file to a new folder with a specified name.
    Returns the new file ID or None if failed.
    """
    try:
        copied_file = (
            drive_service.files()
            .copy(fileId=file_id, body={"name": new_name, "parents": [new_parent_id]}, fields="id")
            .execute()
        )
        return copied_file.get("id")
    except Exception as e:
        logging.error(f"Failed to copy file '{new_name}': {e}")
        return None


def delete_file(drive_service, file_id: str):
    """
    Deletes a file by ID.
    """
    try:
        drive_service.files().delete(fileId=file_id).execute()
    except Exception as e:
        logging.error(f"Failed to delete file ID '{file_id}': {e}")


def move_google_sheets_by_year():
    logging.info("Starting move_google_sheets_by_year process...")
    try:
        drive_service = get_drive_service()
    except Exception as e:
        logging.error(f"Failed to initialize Drive service: {e}")
        return

    source_folder_id = os.getenv("GOOGLE_SHEETS_SOURCE_FOLDER", DJ_SETS)
    if not source_folder_id:
        logging.error(
            "Source folder ID not set in environment variable 'GOOGLE_SHEETS_SOURCE_FOLDER' or DJ_SETS constant."
        )
        return

    try:
        files = list_google_sheets_in_folder(drive_service, source_folder_id)
    except Exception as e:
        logging.error(f"Failed to list files in source folder: {e}")
        return

    logging.info(f"Found {len(files)} Google Sheets files in source folder.")

    for file in files:
        file_name = file.get("name", "")
        file_id = file.get("id")
        year = extract_year_from_filename(file_name)
        if not year:
            logging.debug(f"Skipping file '{file_name}' as it does not start with YYYY-MM-DD.")
            continue

        try:
            year_folder_id = get_or_create_year_folder(drive_service, DJ_SETS, year)
        except Exception as e:
            logging.error(f"Failed to get or create year folder '{year}': {e}")
            continue

        logging.info(f"Copying file '{file_name}' to year folder '{year}'...")
        new_file_id = copy_file_to_folder(drive_service, file_id, year_folder_id, file_name)
        if not new_file_id:
            logging.error(f"Skipping file '{file_name}' due to copy failure.")
            continue

        logging.info(f"Cleaning copied file '{file_name}' (ID: {new_file_id})...")
        try:
            normalize_google_sheet(new_file_id)
        except Exception as e:
            logging.error(f"Cleaning failed for file '{file_name}': {e}")
            continue

        logging.info(
            f"Deleting original file '{file_name}' (ID: {file_id}) after successful cleaning..."
        )
        try:
            delete_file(drive_service, file_id)
        except Exception as e:
            logging.error(f"Failed to delete original file '{file_name}': {e}")

    logging.info("Completed move_google_sheets_by_year process.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    move_google_sheets_by_year()
