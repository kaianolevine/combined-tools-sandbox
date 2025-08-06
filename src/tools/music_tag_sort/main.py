import argparse
import os
from tools.music_tag_sort.renamer import rename_files_in_directory
from tools.music_tag_sort.drive_handler import process_drive_folder


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
        rename_files_in_directory(local_dir, config)


if __name__ == "__main__":
    main()
