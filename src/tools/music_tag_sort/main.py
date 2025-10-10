import argparse
import os
from tools.music_tag_sort import renamer
import core.google_drive as drive
import tempfile

SOURCE_FOLDER_ID = "YOUR_SOURCE_FOLDER_ID"
DEST_FOLDER_ID = "YOUR_DEST_FOLDER_ID"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=str, help="Local directory containing music files")
    args = parser.parse_args()

    running_in_ci = os.getenv("CI", "false").lower() == "true"

    if running_in_ci:
        print("🚀 Running in CI mode")
        folder_id = os.getenv("FOLDER_ID")
        if not folder_id:
            raise ValueError("❌ FOLDER_ID must be set when running in CI mode.")
        config = {"FOLDER_ID": folder_id}
        process_drive_folder(config)
    else:
        print("🚀 Running in LOCAL mode")
        local_dir = args.directory or "./music"
        print(f"🎵 Renaming files in local directory: {local_dir}")
        config = {"rename_order": ["bpm", "title", "artist"], "separator": "__"}
        renamer.rename_files_in_directory(local_dir, config)


def process_drive_folder():
    service = drive.get_drive_service()
    music_files = drive.list_music_files(service, SOURCE_FOLDER_ID)

    for file in music_files:
        temp_path = os.path.join(tempfile.gettempdir(), file["name"])
        drive.download_file(service, file["id"], temp_path)
        print(f"Downloaded: {file['name']}")
        renamed_path = renamer.rename_music_file(temp_path, tempfile.gettempdir())
        print(f"Renamed to: {os.path.basename(renamed_path)}")
        drive.upload_file(service, renamed_path, DEST_FOLDER_ID)
        print(f"Uploaded: {os.path.basename(renamed_path)}")


if __name__ == "__main__":
    main()
