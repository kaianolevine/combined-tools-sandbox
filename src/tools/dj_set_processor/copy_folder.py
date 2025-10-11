from googleapiclient.errors import HttpError
import config
import core.google_drive as google_drive
import core.logger as log


def copy_drive_folder_recursive(source_folder_id, destination_folder_id, service):
    folder_map = {}

    def copy_folder(src_id, dest_id):
        log.debug(f"📂 Scanning folder ID: {src_id}")
        query = f"'{src_id}' in parents and trashed = false"
        page_token = None
        while True:
            try:
                response = (
                    service.files()
                    .list(
                        q=query,
                        spaces="drive",
                        fields="nextPageToken, files(id, name, mimeType)",
                        pageToken=page_token,
                        supportsAllDrives=True,
                    )
                    .execute()
                )
            except HttpError as e:
                log.error(f"❌ Failed to list files in folder ID {src_id}: {e}")
                break

            for item in response.get("files", []):
                name = item["name"]
                item_id = item["id"]
                if item["mimeType"] == "application/vnd.google-apps.folder":
                    try:
                        log.debug(f"📁 Creating folder '{name}' under parent ID {dest_id}")
                        new_folder = (
                            service.files()
                            .create(
                                body={
                                    "name": name,
                                    "mimeType": "application/vnd.google-apps.folder",
                                    "parents": [dest_id],
                                },
                                fields="id",
                                supportsAllDrives=True,
                            )
                            .execute()
                        )
                        new_id = new_folder["id"]
                        folder_map[item_id] = new_id
                        log.info(f"📁 Created folder: {name} (ID: {new_id})")
                        copy_folder(item_id, new_id)
                    except HttpError as e:
                        log.error(
                            f"❌ Failed to create folder '{name}' (ID: {item_id}) in parent {dest_id}: {e}"
                        )
                else:
                    try:
                        log.debug(
                            f"🔄 Attempting to copy file '{name}' (ID: {item_id}) to folder ID {dest_id}"
                        )
                        service.files().copy(
                            fileId=item_id,
                            body={"name": name, "parents": [dest_id]},
                            supportsAllDrives=True,
                        ).execute()
                        log.info(f"📄 Copied file: {name}")
                    except HttpError as e:
                        log.error(
                            f"❌ Failed to copy file '{name}' (ID: {item_id}) to {dest_id}: {e}"
                        )

            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break

    copy_folder(source_folder_id, destination_folder_id)


if __name__ == "__main__":
    log.info("🔄 Authenticating...")
    drive_service = google_drive.get_drive_service()
    log.info("🚀 Starting recursive copy...")
    copy_drive_folder_recursive(
        config.DJ_SETS_FOLDER_ID_OLD, config.DJ_SETS_FOLDER_ID, drive_service
    )
    log.info("✅ All files and folders copied successfully.")
