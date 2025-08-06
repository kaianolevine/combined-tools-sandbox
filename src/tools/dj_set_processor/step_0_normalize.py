# Converted from Google Apps Script to Python

import re
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials

# Configure logging
logging.basicConfig(level=logging.INFO)

# Define the source folder ID and scopes
CSV_FILES = "your-csv-folder-id"  # TODO: Replace with actual folder ID
SCOPES = ["https://www.googleapis.com/auth/drive"]


def normalize_filenames_in_source():
    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    service = build("drive", "v3", credentials=creds)

    renamed = 0
    skipped = 0
    date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")

    try:
        # Get all files in the folder
        results = (
            service.files()
            .list(q=f"'{CSV_FILES}' in parents and trashed = false", fields="files(id, name)")
            .execute()
        )
        files = results.get("files", [])

        for file in files:
            original_name = file["name"].strip()
            match = date_pattern.search(original_name)

            if not match:
                skipped += 1
                continue

            index = original_name.find(match.group(0))
            normalized_name = original_name[index:].strip()

            if normalized_name != original_name:
                try:
                    service.files().update(
                        fileId=file["id"], body={"name": normalized_name}
                    ).execute()
                    logging.info(f'✏️ Renamed "{original_name}" → "{normalized_name}"')
                    renamed += 1
                except HttpError as error:
                    logging.warning(f'⚠️ Failed to rename "{original_name}": {error}')
            else:
                skipped += 1

    except HttpError as error:
        logging.error(f"❌ API error occurred: {error}")

    logging.info(f"✅ Normalization complete. Renamed: {renamed}, Skipped: {skipped}")


if __name__ == "__main__":
    normalize_filenames_in_source()
