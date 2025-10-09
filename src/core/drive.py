from googleapiclient.http import MediaIoBaseDownload
import io
from core import google_api
from core import logger as log

log = log.get_logger()


def get_drive_service():
    return google_api.get_drive_service()


def find_latest_m3u_file(folder_id):
    log.debug(f"Searching for latest .m3u file in folder: {folder_id}")
    service = get_drive_service()
    query = (
        f"'{folder_id}' in parents and name contains '.m3u' and "
        "mimeType != 'application/vnd.google-apps.folder'"
    )
    log.debug(f"Drive query: {query}")
    results = (
        service.files()
        .list(
            q=query,
            pageSize=10,
            fields="files(id, name, modifiedTime)",
            orderBy="modifiedTime desc",
        )
        .execute()
    )
    items = results.get("files", [])
    log.debug(f"Found {len(items)} .m3u files.")
    if items:
        log.debug(f"Latest .m3u file: {items[0]}")
    else:
        log.debug("No .m3u files found.")
    return items[0] if items else None


def download_file(file_id, destination_path):
    """Download a file from Google Drive by ID using the Drive API."""
    log.debug(f"Starting download for file_id={file_id} to {destination_path}")
    service = get_drive_service()

    # Prepare request for file download
    request = service.files().get_media(fileId=file_id)

    # Attempt to open the destination file for writing
    try:
        fh = io.FileIO(destination_path, "wb")
    except Exception as e:
        log.exception(f"Failed to open destination file {destination_path}")
        raise IOError(f"Could not create or write to file: {destination_path}") from e

    # Download file in chunks
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    chunk_count = 0
    while not done:
        status, done = downloader.next_chunk()
        chunk_count += 1
        log.debug(f"Chunk {chunk_count}: Download progress {int(status.progress() * 100)}%")
        print(f"⬇️  Download {int(status.progress() * 100)}%.")
    log.debug(f"Download complete for file_id={file_id} to {destination_path}")


# New function: list_files_in_folder
def list_files_in_folder(folder_id, mime_type_filter=None):
    log.debug(f"Listing files in folder: {folder_id}")
    service = get_drive_service()

    query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder'"
    if mime_type_filter:
        query += f" and mimeType = '{mime_type_filter}'"

    log.debug(f"Drive query: {query}")
    results = (
        service.files()
        .list(
            q=query,
            pageSize=1000,
            fields="files(id, name, modifiedTime)",
            orderBy="modifiedTime desc",
        )
        .execute()
    )
    items = results.get("files", [])
    log.debug(f"Found {len(items)} files in folder.")
    return items
