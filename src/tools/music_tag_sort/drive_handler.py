from tools.music_tag_sort.renamer import rename_music_file
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import tempfile
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload


SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_PATH = "credentials.json"
SOURCE_FOLDER_ID = "YOUR_SOURCE_FOLDER_ID"
DEST_FOLDER_ID = "YOUR_DEST_FOLDER_ID"


def download_and_rename_files_from_drive(folder_id, local_output_dir):
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)

    file_list = drive.ListFile({"q": f"'{folder_id}' in parents and trashed=false"}).GetList()

    for file in file_list:
        _, temp_path = tempfile.mkstemp()
        file.GetContentFile(temp_path)
        print(f"Downloaded: {file['title']}")
        new_path = rename_music_file(temp_path, local_output_dir)
        print(f"Renamed: {new_path}")


def authenticate():
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


def list_music_files(service, folder_id):
    query = f"'{folder_id}' in parents and mimeType contains 'audio'"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get("files", [])


def download_file(service, file_id, dest_path):
    request = service.files().get_media(fileId=file_id)
    with open(dest_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()


def upload_file(service, filepath, folder_id):
    file_metadata = {"name": os.path.basename(filepath), "parents": [folder_id]}
    media = MediaFileUpload(filepath, resumable=True)
    service.files().create(body=file_metadata, media_body=media, fields="id").execute()


def process_drive_folder():
    service = authenticate()
    music_files = list_music_files(service, SOURCE_FOLDER_ID)

    for file in music_files:
        temp_path = os.path.join(tempfile.gettempdir(), file["name"])
        download_file(service, file["id"], temp_path)
        print(f"Downloaded: {file['name']}")
        renamed_path = rename_music_file(temp_path, tempfile.gettempdir())
        print(f"Renamed to: {os.path.basename(renamed_path)}")
        upload_file(service, renamed_path, DEST_FOLDER_ID)
        print(f"Uploaded: {os.path.basename(renamed_path)}")
