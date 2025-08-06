from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from your_package.westie_radio import config
import io
import logging
import os
import pickle

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_drive_service():
    logger.debug("Starting get_drive_service()")

    creds = None
    token_path = "token.pickle"

    # Try loading token from pickle file
    if os.path.exists(token_path):
        logger.debug(f"Loading credentials from {token_path}")
        with open(token_path, "rb") as token:
            creds = pickle.load(token)

    # If no valid creds, load from service account
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.debug("Refreshing expired credentials")
            creds.refresh(Request())
        else:
            logger.debug("Loading credentials from service account")
            creds = service_account.Credentials.from_service_account_file(
                "credentials.json", scopes=config.SCOPES
            )

    return build("drive", "v3", credentials=creds)


def find_latest_m3u_file(folder_id):
    logger.debug(f"Searching for latest .m3u file in folder: {folder_id}")
    service = get_drive_service()
    query = (
        f"'{folder_id}' in parents and name contains '.m3u' and "
        "mimeType != 'application/vnd.google-apps.folder'"
    )
    logger.debug(f"Drive query: {query}")
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
    logger.debug(f"Found {len(items)} .m3u files.")
    if items:
        logger.debug(f"Latest .m3u file: {items[0]}")
    else:
        logger.debug("No .m3u files found.")
    return items[0] if items else None


def download_file(file_id, destination_path):
    """Download a file from Google Drive by ID using the Drive API."""
    logger.debug(f"Starting download for file_id={file_id} to {destination_path}")
    service = get_drive_service()

    # Prepare request for file download
    request = service.files().get_media(fileId=file_id)

    # Attempt to open the destination file for writing
    try:
        fh = io.FileIO(destination_path, "wb")
    except Exception as e:
        logger.exception(f"Failed to open destination file {destination_path}")
        raise IOError(f"Could not create or write to file: {destination_path}") from e

    # Download file in chunks
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    chunk_count = 0
    while not done:
        status, done = downloader.next_chunk()
        chunk_count += 1
        logger.debug(f"Chunk {chunk_count}: Download progress {int(status.progress() * 100)}%")
        print(f"⬇️  Download {int(status.progress() * 100)}%.")
    logger.debug(f"Download complete for file_id={file_id} to {destination_path}")


# New function: list_files_in_folder
def list_files_in_folder(folder_id, mime_type_filter=None):
    logger.debug(f"Listing files in folder: {folder_id}")
    service = get_drive_service()

    query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder'"
    if mime_type_filter:
        query += f" and mimeType = '{mime_type_filter}'"

    logger.debug(f"Drive query: {query}")
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
    logger.debug(f"Found {len(items)} files in folder.")
    return items
