from googleapiclient.http import MediaIoBaseDownload
import io
from core import google_api
from core import logger as log

log = log.get_logger()


def get_drive_service():
    log.debug("get_drive_service called with no parameters")
    service = google_api.get_drive_service()
    log.debug("Drive service obtained")
    return service


def find_latest_m3u_file(folder_id):
    log.debug(f"find_latest_m3u_file called with folder_id={folder_id}")
    log.info(f"Searching for latest .m3u file in folder: {folder_id}")
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
    log.info(f"Found {len(items)} .m3u files in folder {folder_id}.")
    if items:
        log.info(f"Latest .m3u file: ID={items[0]['id']}, Name={items[0]['name']}")
        log.debug(f"Latest .m3u file details: {items[0]}")
    else:
        log.info("No .m3u files found.")
    return items[0] if items else None


def download_file(file_id, destination_path):
    """Download a file from Google Drive by ID using the Drive API."""
    log.debug(f"download_file called with file_id={file_id}, destination_path={destination_path}")
    log.info(f"Starting download for file_id={file_id} to {destination_path}")
    service = get_drive_service()

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


# New function: list_files_in_folder
def list_files_in_folder(folder_id, mime_type_filter=None):
    log.debug(
        f"list_files_in_folder called with folder_id={folder_id}, mime_type_filter={mime_type_filter}"
    )
    log.info(f"Listing files in folder: {folder_id}")
    service = get_drive_service()

    query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder'"
    if mime_type_filter:
        query += f" and mimeType = '{mime_type_filter}'"
        log.info(f"Applying mime_type_filter: {mime_type_filter}")

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
    log.info(f"Found {len(items)} files in folder {folder_id}.")
    if items:
        file_details = ", ".join([f"{item['id']}:{item['name']}" for item in items])
        log.debug(f"Files found: {file_details}")
    else:
        log.debug("No files found in folder.")
    return items
