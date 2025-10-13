import io
import core._google_credentials as google_api
import core.google_sheets as google_sheets
from core import logger as log
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from typing import List, Dict
import os
from googleapiclient.errors import HttpError


log = log.get_logger()
FOLDER_CACHE = {}


def get_drive_service():
    return google_api.get_drive_client()


def extract_date_from_filename(filename):
    import re

    match = re.match(r"(\d{4}-\d{2}-\d{2})", filename)
    return match.group(1) if match else filename


def list_files_in_folder(
    service,
    folder_id,
    mime_type_filter=None,
    include_all_drives=True,
    include_folders=False,
):
    """
    List files in a Google Drive folder, optionally filtering by MIME type.
    Supports both standard Drive and Shared Drive contexts.
    :param service: Google Drive API service instance.
    :param folder_id: The ID of the folder to list files from.
    :param mime_type_filter: Optional. If set, restricts results to this MIME type.
    :param include_all_drives: If True (default), includes files from all drives (Shared Drives support).
    :param include_folders: If False (default), excludes folders from results.
    :return: List of file dictionaries.
    """
    log.debug(
        f"list_files_in_folder called with folder_id={folder_id}, "
        f"mime_type_filter={mime_type_filter}, include_all_drives={include_all_drives}, include_folders={include_folders}"
    )
    query = f"'{folder_id}' in parents"
    if not include_folders:
        query += " and mimeType != 'application/vnd.google-apps.folder'"
    if mime_type_filter:
        query += f" and mimeType = '{mime_type_filter}'"
        log.info(f"Applying mime_type_filter: {mime_type_filter}")
    query += " and trashed = false"
    log.debug(f"Drive query: {query}")
    files = []
    page_token = None
    while True:
        try:
            params = {
                "q": query,
                "fields": "nextPageToken, files(id, name, mimeType, modifiedTime)",
                "pageToken": page_token,
                "orderBy": "modifiedTime desc",
            }
            if include_all_drives:
                params["supportsAllDrives"] = True
                params["includeItemsFromAllDrives"] = True
                params["spaces"] = "drive"
            result = service.files().list(**params).execute()
            batch = result.get("files", [])
            files.extend(batch)
            page_token = result.get("nextPageToken", None)
            log.debug(
                f"Fetched {len(batch)} files (cumulative total: {len(files)}) from folder {folder_id}"
            )
            if page_token is None:
                break
        except Exception as e:
            log.error(f"Error listing files in folder {folder_id}: {e}")
            break
    log.info(f"Found {len(files)} files in folder {folder_id}.")
    if files:
        file_details = ", ".join(
            [f"{item.get('id', '?')}:{item.get('name', '?')}" for item in files]
        )
        log.debug(f"Files found: {file_details}")
    else:
        log.debug("No files found in folder.")
    return files


def list_music_files(service, folder_id):
    query = f"'{folder_id}' in parents and mimeType contains 'audio'"
    results = (
        service.files()
        .list(
            q=query,
            spaces="drive",
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    return results.get("files", [])


def get_or_create_folder(parent_folder_id: str, name: str, drive_service) -> str:
    """Returns the folder ID for a subfolder under parent_folder_id with the given name.
    Creates it if it doesn't exist, using an in-memory cache to avoid duplication."""
    cache_key = f"{parent_folder_id}/{name}"
    if cache_key in FOLDER_CACHE:
        return FOLDER_CACHE[cache_key]

    # Search for existing folder
    query = (
        f"'{parent_folder_id}' in parents and "
        f"name = '{name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    )
    response = (
        drive_service.files()
        .list(
            q=query,
            spaces="drive",
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    folders = response.get("files", [])
    if folders:
        print(f"📁 Found existing folder '{name}' under parent {parent_folder_id}")
        folder_id = folders[0]["id"]
    else:
        print(f"📁 Creating new folder '{name}' under parent {parent_folder_id}")
        folder_metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id],
        }
        folder = (
            drive_service.files()
            .create(body=folder_metadata, fields="id", supportsAllDrives=True)
            .execute()
        )
        folder_id = folder["id"]

    FOLDER_CACHE[cache_key] = folder_id
    return folder_id


def get_or_create_subfolder(drive_service, parent_folder_id, subfolder_name):
    """
    Gets or creates a subfolder inside a shared drive or My Drive.
    Returns the folder ID.
    """
    query = (
        f"mimeType='application/vnd.google-apps.folder' and "
        f"name='{subfolder_name}' and "
        f"'{parent_folder_id}' in parents and trashed=false"
    )
    response = (
        drive_service.files()
        .list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )

    files = response.get("files", [])
    if files:
        return files[0]["id"]

    file_metadata = {
        "name": subfolder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_folder_id],
    }
    folder = (
        drive_service.files()
        .create(body=file_metadata, fields="id", supportsAllDrives=True)
        .execute()
    )

    return folder.get("id")


def get_file_by_name(drive_service, folder_id, filename):
    """
    Returns the file metadata for a file with a given name in a folder, or None if not found.
    """
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    response = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = response.get("files", [])
    if files:
        return files[0]
    return None


def get_all_subfolders(drive_service, parent_folder_id: str) -> List[Dict]:
    """
    Returns a list of all subfolders in the specified parent folder.
    Supports Shared Drives.
    """
    log.info(
        f"📂 Retrieving all subfolders in folder ID {parent_folder_id} (shared drives enabled)"
    )
    try:
        query = f"'{parent_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        folders = []
        page_token = None
        while True:
            response = (
                drive_service.files()
                .list(
                    q=query,
                    spaces="drive",
                    fields="nextPageToken, files(id, name)",
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                )
                .execute()
            )
            folders.extend(response.get("files", []))
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break
        log.info(f"📂 Found {len(folders)} subfolders under {parent_folder_id}")
        return folders
    except HttpError as error:
        log.error(f"An error occurred while retrieving subfolders: {error}")
        raise


def get_files_in_folder(service, folder_id, name_contains=None, mime_type=None, trashed=False):
    """Returns a list of files in a Google Drive folder, optionally filtering by name substring and MIME type."""
    query = f"'{folder_id}' in parents"
    if name_contains:
        query += f" and name contains '{name_contains}'"
    if mime_type:
        query += f" and mimeType = '{mime_type}'"
    if trashed is False:
        query += " and trashed = false"

    results = (
        service.files()
        .list(
            q=query,
            spaces="drive",
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )

    return results.get("files", [])


def download_file(service, file_id, destination_path):
    """Download a file from Google Drive by ID using the Drive API."""
    log.debug(f"download_file called with file_id={file_id}, destination_path={destination_path}")
    log.info(f"Starting download for file_id={file_id} to {destination_path}")

    log.debug("Preparing request for file download")
    # Prepare request for file download
    request = service.files().get_media(fileId=file_id)

    # Attempt to open the destination file for writing
    try:
        fh = io.FileIO(destination_path, "wb")
        log.debug(f"Destination file {destination_path} opened for writing")
    except Exception as e:
        log.exception(f"Failed to open destination file {destination_path}")
        raise IOError(f"Could not create or write to file: {destination_path}") from e

    # Download file in chunks
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    chunk_count = 0
    log.info("Beginning chunked download")
    while not done:
        status, done = downloader.next_chunk()
        chunk_count += 1
        progress_percent = int(status.progress() * 100) if status else 0
        log.debug(f"Chunk {chunk_count}: Download progress {progress_percent}%")
        print(f"⬇️  Download {progress_percent}%.")
    log.info(f"Download complete for file_id={file_id} to {destination_path}")
    log.debug(f"Total chunks downloaded: {chunk_count}")


def upload_file(service, filepath, folder_id):
    log.debug(f"Filepath: {filepath}, Folder ID: {folder_id}")
    file_metadata = {"name": os.path.basename(filepath), "parents": [folder_id]}
    media = MediaFileUpload(filepath, resumable=True)
    service.files().create(
        body=file_metadata, media_body=media, fields="id", supportsAllDrives=True
    ).execute()


def upload_to_drive(drive, filepath, parent_id):
    log.debug(f"Uploading file '{filepath}' to Drive folder ID '{parent_id}'")
    file_metadata = {
        "name": os.path.basename(filepath),
        "parents": [parent_id],
        "mimeType": "application/vnd.google-apps.spreadsheet",
    }
    media = MediaFileUpload(filepath, mimetype="text/csv")
    uploaded = (
        drive.files()
        .create(body=file_metadata, media_body=media, fields="id", supportsAllDrives=True)
        .execute()
    )
    log.info(f"📄 Uploaded to Drive as Google Sheet: {filepath}")
    log.debug(f"Uploaded file ID: {uploaded['id']}")

    # TODO - move this to a sheets function
    # After uploading, use gspread to open the sheet and check for 'sep=' in first row of all worksheets
    gc = google_sheets.get_gspread_client()
    spreadsheet = gc.open_by_key(uploaded["id"])
    for sheet in spreadsheet.worksheets():
        first_row = sheet.row_values(1)
        if first_row and first_row[0].strip().lower().startswith("sep="):
            sheet.delete_rows(1)

    return uploaded["id"]


def create_spreadsheet(
    drive_service,
    name,
    parent_folder_id,
    mime_type: str = "application/vnd.google-apps.spreadsheet",
):
    """
    Finds a file by name in the specified folder. If not found, creates a new file with that name.
    This function supports Shared Drives (supportsAllDrives=True).
    Returns the file ID.
    """
    log.info(
        f"🔍 Searching for file '{name}' in folder ID {parent_folder_id} (shared drives enabled)"
    )
    try:
        query = f"'{parent_folder_id}' in parents and name = '{name}' and mimeType = '{mime_type}' and trashed = false"
        response = (
            drive_service.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files = response.get("files", [])
        if files:
            log.info(f"📄 Found existing file '{name}' with ID {files[0]['id']}")
            return files[0]["id"]
        else:
            log.info(
                f"➕ No existing file named '{name}' — creating new one in parent {parent_folder_id}"
            )
            file_metadata = {"name": name, "mimeType": mime_type, "parents": [parent_folder_id]}
            file = (
                drive_service.files()
                .create(body=file_metadata, fields="id", supportsAllDrives=True)
                .execute()
            )
            log.info(f"🆕 Created new file '{name}' with ID {file['id']}")
            return file["id"]
    except HttpError as error:
        log.error(f"An error occurred while finding or creating file: {error}")
        raise


def move_file_to_folder(drive_service, file_id, folder_id):
    """
    Moves a file to a specified folder.
    """
    # Get current parents
    file = drive_service.files().get(fileId=file_id, fields="parents").execute()
    previous_parents = ",".join(file.get("parents", []))
    # Move the file to the new folder
    drive_service.files().update(
        fileId=file_id, addParents=folder_id, removeParents=previous_parents, fields="id, parents"
    ).execute()


def remove_file_from_root(drive_service, file_id):
    """
    Removes a file from the root folder.
    """
    file = drive_service.files().get(fileId=file_id, fields="parents").execute()
    parents = file.get("parents", [])
    if "root" in parents:
        drive_service.files().update(
            fileId=file_id, removeParents="root", fields="id, parents"
        ).execute()


def find_or_create_file_by_name(
    drive_service,
    name: str,
    parent_folder_id: str,
    mime_type: str = "application/vnd.google-apps.spreadsheet",
) -> str:
    """
    Finds a file by name in the specified folder. If not found, creates a new file with that name.
    This function supports Shared Drives (supportsAllDrives=True).
    Returns the file ID.
    """
    log.info(
        f"🔍 Searching for file '{name}' in folder ID {parent_folder_id} (shared drives enabled)"
    )
    try:
        query = f"'{parent_folder_id}' in parents and name = '{name}' and mimeType = '{mime_type}' and trashed = false"
        response = (
            drive_service.files()
            .list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files = response.get("files", [])
        if files:
            log.info(f"📄 Found existing file '{name}' with ID {files[0]['id']}")
            return files[0]["id"]
        else:
            log.info(
                f"➕ No existing file named '{name}' — creating new one in parent {parent_folder_id}"
            )
            file_metadata = {"name": name, "mimeType": mime_type, "parents": [parent_folder_id]}
            file = (
                drive_service.files()
                .create(body=file_metadata, fields="id", supportsAllDrives=True)
                .execute()
            )
            log.info(f"🆕 Created new file '{name}' with ID {file['id']}")
            return file["id"]
    except HttpError as error:
        log.error(f"An error occurred while finding or creating file: {error}")
        raise


def find_subfolder_id(service, parent_folder_id: str, subfolder_name: str) -> str | None:
    """
    Finds the ID of a subfolder with the given name inside the specified parent folder.

    Args:
        service: An authorized Google Drive service instance.
        parent_folder_id (str): The ID of the parent folder to search within.
        subfolder_name (str): The exact name of the subfolder to find.

    Returns:
        str | None: The ID of the subfolder if found, otherwise None.
    """
    try:
        query = (
            f"'{parent_folder_id}' in parents and "
            f"mimeType = 'application/vnd.google-apps.folder' and "
            f"name = '{subfolder_name}' and trashed = false"
        )
        response = (
            service.files()
            .list(
                q=query,
                spaces="drive",
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=10,
            )
            .execute()
        )
        files = response.get("files", [])
        if files:
            return files[0]["id"]
    except Exception as e:
        log.error(f"❌ Error finding subfolder '{subfolder_name}': {e}")
    return None
