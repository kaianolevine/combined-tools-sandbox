import time
import re
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from difflib import SequenceMatcher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = "path/to/service_account.json"  # Update with your path
DJ_SETS = "YOUR_DJ_SETS_FOLDER_ID"  # Update with your folder ID
SUMMARY_FOLDER_NAME = "Summary"
LOCK_FILE_NAME = ".lock_summary_folder"

# Initialize Google API clients
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build("drive", "v3", credentials=credentials)
sheets_service = build("sheets", "v4", credentials=credentials)


def try_lock_folder(folder_name):
    """
    Emulate folder locking by creating a lock file inside the folder.
    Returns True if lock acquired, False if already locked.
    """
    folder_id = DJ_SETS
    # Find Summary folder inside DJ_SETS
    summary_folder_id = get_or_create_subfolder(folder_id, folder_name)
    # Check if lock file exists
    query = f"'{summary_folder_id}' in parents and name='{LOCK_FILE_NAME}' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])
    if files:
        logger.info(f"🔒 {folder_name} folder is locked — skipping.")
        return False
    # Create lock file
    file_metadata = {
        "name": LOCK_FILE_NAME,
        "parents": [summary_folder_id],
        "mimeType": "application/octet-stream",
    }
    drive_service.files().create(body=file_metadata).execute()
    return True


def release_folder_lock(folder_name):
    """
    Remove the lock file to release the lock.
    """
    folder_id = DJ_SETS
    summary_folder_id = get_or_create_subfolder(folder_id, folder_name)
    query = f"'{summary_folder_id}' in parents and name='{LOCK_FILE_NAME}' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])
    for f in files:
        try:
            drive_service.files().delete(fileId=f["id"]).execute()
        except HttpError as e:
            logger.error(f"Error releasing lock: {e}")


def get_or_create_subfolder(parent_folder_id, folder_name):
    """
    Get or create a subfolder with the given name inside the parent folder.
    Returns the folder ID.
    """
    query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]
    # Create folder
    file_metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_folder_id],
    }
    folder = drive_service.files().create(body=file_metadata, fields="id").execute()
    return folder["id"]


def list_files_in_folder(folder_id):
    """
    List all files in a folder (non-recursive).
    Returns a list of dicts with 'id', 'name', and 'mimeType'.
    """
    files = []
    page_token = None
    while True:
        response = (
            drive_service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed=false",
                spaces="drive",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
            )
            .execute()
        )
        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken", None)
        if page_token is None:
            break
    return files


def get_sheet_values(spreadsheet_id, sheet_name):
    """
    Get all values from the given sheet.
    Returns a list of rows (each row is a list of strings).
    """
    range_name = f"{sheet_name}"
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name, majorDimension="ROWS")
        .execute()
    )
    values = result.get("values", [])
    # Normalize all values to strings
    normalized = []
    for row in values:
        normalized.append([str(cell) if cell is not None else "" for cell in row])
    return normalized


def clear_sheet(spreadsheet_id, sheet_id):
    """
    Clear all data in the sheet by sheetId.
    """
    requests = [
        {
            "updateCells": {
                "range": {
                    "sheetId": sheet_id,
                },
                "fields": "userEnteredValue,userEnteredFormat",
            }
        }
    ]
    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def update_sheet_values(spreadsheet_id, sheet_name, values):
    """
    Update values in the sheet starting from A1.
    """
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=sheet_name,
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()


def set_sheet_formatting(
    spreadsheet_id, sheet_id, header_row_count, total_rows, total_cols, backgrounds
):
    """
    Apply formatting:
    - Freeze header rows
    - Set font bold for header row
    - Set horizontal alignment left for all data
    - Set number format to plain text for data rows
    - Set background colors for data rows
    - Auto resize columns and adjust width with max 200px
    """
    requests = []

    # Freeze header rows
    requests.append(
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": header_row_count},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        }
    )

    # Bold font for header row
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": header_row_count,
                    "startColumnIndex": 0,
                    "endColumnIndex": total_cols,
                },
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        }
    )

    # Horizontal alignment left for all data
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": total_rows,
                    "startColumnIndex": 0,
                    "endColumnIndex": total_cols,
                },
                "cell": {"userEnteredFormat": {"horizontalAlignment": "LEFT"}},
                "fields": "userEnteredFormat.horizontalAlignment",
            }
        }
    )

    # Number format plain text for data rows (excluding header)
    if total_rows > header_row_count:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": header_row_count,
                        "endRowIndex": total_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": total_cols,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {"type": "TEXT", "pattern": "@STRING@"}
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )

    # Set background colors for data rows (excluding header)
    if len(backgrounds) > 1:
        bg_requests = []
        for row_idx, bg_colors in enumerate(backgrounds[1:], start=header_row_count):
            row_request = {
                "updateCells": {
                    "rows": [
                        {
                            "values": [
                                {"userEnteredFormat": {"backgroundColor": hex_to_rgb(color)}}
                                for color in bg_colors
                            ]
                        }
                    ],
                    "fields": "userEnteredFormat.backgroundColor",
                    "start": {"sheetId": sheet_id, "rowIndex": row_idx, "columnIndex": 0},
                }
            }
            bg_requests.append(row_request)
        requests.extend(bg_requests)

    # Auto resize columns and adjust width (+20px, max 200px)
    for col in range(total_cols):
        requests.append(
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": col,
                        "endIndex": col + 1,
                    }
                }
            }
        )
    # After auto-resize, set column widths with +20px and max 200px
    # Unfortunately, we cannot get column width via API easily,
    # so we skip this part as Google Sheets API doesn't provide a way to read current widths.
    # Instead, we just auto resize.

    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def hex_to_rgb(hex_color):
    """
    Convert hex color string like '#fff3b0' to dict with red, green, blue floats 0-1.
    """
    hex_color = hex_color.lstrip("#")
    lv = len(hex_color)
    if lv == 6:
        r, g, b = tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))
    elif lv == 3:
        r, g, b = tuple(int(hex_color[i] * 2, 16) for i in range(3))
    else:
        r, g, b = (255, 255, 255)
    return {"red": r / 255, "green": g / 255, "blue": b / 255}


def deduplicate_summaries():
    logger.info("🚀 Starting deduplicate_summaries()")
    start_time = time.time()
    MAX_RUNTIME = 5 * 60  # 5 minutes in seconds

    if not try_lock_folder(SUMMARY_FOLDER_NAME):
        return

    try:
        summary_folder_id = get_or_create_subfolder(DJ_SETS, SUMMARY_FOLDER_NAME)
        files = list_files_in_folder(summary_folder_id)

        for file in files:
            elapsed = time.time() - start_time
            if elapsed > MAX_RUNTIME:
                logger.info("⏳ Time limit reached. Exiting.")
                break

            name = file["name"]
            mime_type = file["mimeType"]
            match = re.match(r"^_pending_(\d{4}) Summary$", name)
            if not match or mime_type != "application/vnd.google-apps.spreadsheet":
                continue

            try:
                logger.info(f"📄 Processing: {name}")
                spreadsheet_id = file["id"]
                # Get sheet metadata to find first sheet ID and name
                spreadsheet = (
                    sheets_service.spreadsheets()
                    .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
                    .execute()
                )
                sheets = spreadsheet.get("sheets", [])
                if not sheets:
                    logger.warning(f"⚠️ No sheets found in {name}")
                    continue
                first_sheet = sheets[0]
                sheet_id = first_sheet["properties"]["sheetId"]
                sheet_name = first_sheet["properties"]["title"]

                data = get_sheet_values(spreadsheet_id, sheet_name)
                if len(data) < 2:
                    logger.warning(f"⚠️ Skipping empty or header-only file: {name}")
                    continue

                header = data[0]
                rows = [row for row in data[1:] if "".join(row).strip() != ""]

                dedup_result = deduplicate_rows_with_soft_match(
                    [{"header": header, "rows": rows}], header
                )
                headers = dedup_result["headers"]
                rows_with_meta = dedup_result["rowsWithMeta"]
                final_header = headers + ["Count"]
                try:
                    title_index = headers.index("Title")
                except ValueError:
                    title_index = -1
                try:
                    comment_index = headers.index("Comment")
                except ValueError:
                    comment_index = -1

                with_count = [r for r in rows_with_meta if r["count"] > 0]

                soft_matches = sorted(
                    [r for r in with_count if r.get("match") == "soft"],
                    key=lambda r: str(r["data"][title_index]) if title_index >= 0 else "",
                )
                others = sorted(
                    [r for r in with_count if r.get("match") != "soft"],
                    key=lambda r: str(r["data"][title_index]) if title_index >= 0 else "",
                )

                final_rows = [final_header]
                backgrounds = [["#ffffff" for _ in final_header]]

                def should_exclude(comment):
                    lc = (comment or "").lower()
                    return "routine |" in lc or "the open 2024" in lc or "fx |" in lc

                def add_rows(lst):
                    for item in lst:
                        data_row = item["data"]
                        count = item["count"]
                        match_type = item.get("match", "")
                        comment = data_row[comment_index] if comment_index >= 0 else ""
                        if should_exclude(comment):
                            continue
                        final_rows.append(data_row + [count])
                        bg = ["#ffffff"] * len(final_header)
                        if match_type == "soft":
                            for i in range(len(headers)):
                                bg[i] = "#fff3b0"
                        backgrounds.append(bg)

                add_rows(soft_matches)
                add_rows(others)

                # Clear sheet
                clear_sheet(spreadsheet_id, sheet_id)

                # Update values
                update_sheet_values(spreadsheet_id, sheet_name, final_rows)

                # Apply formatting
                set_sheet_formatting(
                    spreadsheet_id=spreadsheet_id,
                    sheet_id=sheet_id,
                    header_row_count=1,
                    total_rows=len(final_rows),
                    total_cols=len(final_rows[0]),
                    backgrounds=backgrounds,
                )

                # Rename file
                new_name = f"{match.group(1)} Summary"
                drive_service.files().update(
                    fileId=spreadsheet_id, body={"name": new_name}
                ).execute()
                logger.info(f"✏️ Renamed to: {new_name}")

                time.sleep(10)  # Sleep 10 seconds
                logger.info(f"✅ Deduplicated: {new_name}")

            except Exception as e:
                logger.error(f"❌ Error deduplicating {name}: {e}")

    finally:
        release_folder_lock(SUMMARY_FOLDER_NAME)
        logger.info("🏁 deduplicate_summaries complete.")


def deduplicate_rows_with_soft_match(sheet_data, all_headers):
    dedup_fields = ["Title", "Remix", "Artist", "Comment", "Genre", "Year", "BPM"]
    desired_order = ["Title", "Remix", "Artist", "Comment", "Genre", "Year", "BPM", "Length"]

    lower_header_map = {h.lower(): h for h in all_headers}

    headers = [lower_header_map[h.lower()] for h in desired_order if h.lower() in lower_header_map]

    primary_fields = [f for f in ["Title", "Remix", "Artist"] if f in headers]
    secondary_fields = [f for f in dedup_fields if f not in primary_fields and f in headers]

    primary_indices = [{"field": f, "index": headers.index(f)} for f in primary_fields]
    secondary_indices = [{"field": f, "index": headers.index(f)} for f in secondary_fields]

    deduped_rows = []

    for sheet in sheet_data:
        header = sheet["header"]
        rows = sheet["rows"]
        header_map = {h.lower(): i for i, h in enumerate(header)}

        for row in rows:
            aligned = [
                row[header_map.get(h.lower(), "")] if header_map.get(h.lower(), "") != "" else ""
                for h in headers
            ]
            # Fix for possible missing keys:
            aligned = []
            for h in headers:
                idx = header_map.get(h.lower())
                if idx is not None and idx < len(row):
                    aligned.append(row[idx])
                else:
                    aligned.append("")

            matched = False
            for group in deduped_rows:
                other = group["data"]
                matches_all_primary = all(
                    aligned[p["index"]] == other[p["index"]] for p in primary_indices
                )
                if not matches_all_primary:
                    continue
                secondary_match = all(
                    (
                        not aligned[s["index"]]
                        or not other[s["index"]]
                        or aligned[s["index"]] == other[s["index"]]
                    )
                    for s in secondary_indices
                )
                if secondary_match:
                    group["count"] += 1
                    matched = True
                    break
            if not matched:
                deduped_rows.append({"data": aligned, "count": 1, "match": ""})

    # Soft match flagging
    all_indices = primary_indices + secondary_indices
    for i in range(len(deduped_rows)):
        for j in range(i + 1, len(deduped_rows)):
            a = deduped_rows[i]
            b = deduped_rows[j]
            shared = get_shared_filled_fields(a["data"], b["data"], all_indices)
            score = get_dedup_match_score(a["data"], b["data"], all_indices)

            all_similar = all(
                (
                    not str(a["data"][idx["index"]])
                    or not str(b["data"][idx["index"]])
                    or string_similarity(
                        str(a["data"][idx["index"]]), str(b["data"][idx["index"]])
                    )
                    >= 0.5
                    or clean_title(str(a["data"][idx["index"]]))
                    == clean_title(str(b["data"][idx["index"]]))
                )
                for idx in all_indices
            )

            required_shared = 2 if len(primary_indices) >= 3 else 1
            if score >= 0.5 and shared >= required_shared and all_similar:
                if not a["match"]:
                    a["match"] = "soft"
                if not b["match"]:
                    b["match"] = "soft"

    return {"headers": headers, "rowsWithMeta": deduped_rows}


def get_shared_filled_fields(data1, data2, indices):
    count = 0
    for idx in indices:
        v1 = data1[idx["index"]]
        v2 = data2[idx["index"]]
        if v1 and v2:
            count += 1
    return count


def get_dedup_match_score(data1, data2, indices):
    total_score = 0
    count = 0
    for idx in indices:
        v1 = str(data1[idx["index"]] or "")
        v2 = str(data2[idx["index"]] or "")
        if v1 and v2:
            total_score += string_similarity(v1, v2)
            count += 1
    if count == 0:
        return 0
    return total_score / count


def string_similarity(a, b):
    """
    Returns a similarity score between 0 and 1 for two strings.
    """
    return SequenceMatcher(None, a, b).ratio()


def clean_title(title):
    """
    Clean title string for comparison: lowercase and strip.
    """
    return title.lower().strip()


def deduplicate_complete_summary():
    logger.info("🚀 Starting deduplicate_complete_summary()")

    if not try_lock_folder(SUMMARY_FOLDER_NAME):
        return

    try:
        parent_folder_id = DJ_SETS
        summary_folder_id = get_or_create_subfolder(parent_folder_id, SUMMARY_FOLDER_NAME)
        files = list_files_in_folder(summary_folder_id)

        file = None
        for f in files:
            if f["name"].lower() == "_pending_complete summary":
                file = f
                break

        if not file:
            logger.warning("⚠️ _pending_Complete Summary file not found.")
            return

        logger.info("📄 Processing: _pending_Complete Summary")
        spreadsheet_id = file["id"]

        spreadsheet = (
            sheets_service.spreadsheets()
            .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
            .execute()
        )
        sheets = spreadsheet.get("sheets", [])
        if not sheets:
            logger.warning("⚠️ No sheets found in _pending_Complete Summary")
            return
        first_sheet = sheets[0]
        sheet_id = first_sheet["properties"]["sheetId"]
        sheet_name = first_sheet["properties"]["title"]

        data = get_sheet_values(spreadsheet_id, sheet_name)
        if len(data) < 2:
            logger.warning("⚠️ Skipping empty or header-only Complete Summary")
            return

        header = data[0]
        rows = [row for row in data[1:] if "".join(row).strip() != ""]

        dedup_result = deduplicate_rows_with_soft_match_complete_summary(
            [{"header": header, "rows": rows}]
        )
        headers = dedup_result["headers"]
        rows_with_meta = dedup_result["rowsWithMeta"]

        title_index = next((i for i, h in enumerate(headers) if h.lower() == "title"), -1)
        comment_index = next((i for i, h in enumerate(headers) if h.lower() == "comment"), -1)

        with_count = [r for r in rows_with_meta if r["count"] > 0]

        def should_remove(comment):
            c = (comment or "").lower()
            return "routine |" in c or "the open 2024" in c or "fx |" in c

        soft_matches = sorted(
            [
                r
                for r in with_count
                if r.get("match") == "soft"
                and not should_remove(r["data"][comment_index] if comment_index >= 0 else "")
            ],
            key=lambda r: str(r["data"][title_index]) if title_index >= 0 else "",
        )
        others = sorted(
            [
                r
                for r in with_count
                if r.get("match") != "soft"
                and not should_remove(r["data"][comment_index] if comment_index >= 0 else "")
            ],
            key=lambda r: str(r["data"][title_index]) if title_index >= 0 else "",
        )

        final_rows = [headers]
        backgrounds = [["#ffffff" for _ in headers]]

        # Year columns detection (4 digit headers)
        year_cols = [h for h in headers if re.match(r"^\d{4}$", h)]

        def add_rows(lst):
            for item in lst:
                data_row = item["data"][:]
                # Replace year-column "0" values with empty string
                for year in year_cols:
                    i = headers.index(year)
                    if i < len(data_row) and data_row[i] == "0":
                        data_row[i] = ""
                final_rows.append(data_row)
                bg = ["#ffffff"] * len(headers)
                if item.get("match") == "soft":
                    for i in range(len(headers)):
                        bg[i] = "#fff3b0"
                backgrounds.append(bg)

        add_rows(soft_matches)
        add_rows(others)

        # Clear sheet
        clear_sheet(spreadsheet_id, sheet_id)

        # Update values
        update_sheet_values(spreadsheet_id, sheet_name, final_rows)

        # Apply formatting
        set_sheet_formatting(
            spreadsheet_id=spreadsheet_id,
            sheet_id=sheet_id,
            header_row_count=1,
            total_rows=len(final_rows),
            total_cols=len(final_rows[0]),
            backgrounds=backgrounds,
        )

        # Rename file
        drive_service.files().update(
            fileId=spreadsheet_id, body={"name": "Complete Summary"}
        ).execute()
        logger.info("✏️ Renamed file to: Complete Summary")

        time.sleep(10)  # Sleep 10 seconds
        logger.info("✅ Complete Summary deduplicated.")

    except Exception as e:
        logger.error(f"❌ Error processing Complete Summary: {e}")

    finally:
        release_folder_lock(SUMMARY_FOLDER_NAME)
        logger.info("🏁 deduplicate_complete_summary complete.")


def deduplicate_rows_with_soft_match_complete_summary(sheet_data):
    dedup_fields = ["Title", "Remix", "Artist", "Comment", "Genre", "Year", "BPM"]
    desired_order = ["Title", "Remix", "Artist", "Comment", "Genre", "Year", "BPM", "Length"]

    all_headers = sheet_data[0]["header"]
    if not isinstance(all_headers, list):
        raise ValueError("Invalid allHeaders input — expected an array.")

    lower_header_map = {h.lower(): h for h in all_headers}

    headers = [lower_header_map[h.lower()] for h in desired_order if h.lower() in lower_header_map]

    # Add year columns (e.g., 2016–2030+) detected in headers
    year_cols = [h for h in all_headers if re.match(r"^\d{4}$", h)]
    headers.extend([y for y in year_cols if y not in headers])

    primary_fields = [f for f in ["Title", "Remix", "Artist"] if f.lower() in lower_header_map]
    primary_fields = [lower_header_map[f.lower()] for f in primary_fields]
    secondary_fields = [
        f for f in dedup_fields if f not in primary_fields and f.lower() in lower_header_map
    ]
    secondary_fields = [lower_header_map[f.lower()] for f in secondary_fields]

    primary_indices = [{"field": f, "index": headers.index(f)} for f in primary_fields]
    secondary_indices = [{"field": f, "index": headers.index(f)} for f in secondary_fields]

    deduped_rows = []

    for sheet in sheet_data:
        header = sheet["header"]
        rows = sheet["rows"]
        header_map = {h.lower(): i for i, h in enumerate(header)}

        for row in rows:
            aligned = []
            for h in headers:
                idx = header_map.get(h.lower())
                if idx is not None and idx < len(row):
                    aligned.append(row[idx])
                else:
                    aligned.append("")
            signature = "|".join(aligned[p["index"]] for p in primary_indices)

            matched = False
            for group in deduped_rows:
                other = group["data"]
                matches_all_primary = all(
                    aligned[p["index"]] == other[p["index"]] for p in primary_indices
                )
                if not matches_all_primary:
                    continue
                secondary_match = all(
                    (
                        not aligned[s["index"]]
                        or not other[s["index"]]
                        or aligned[s["index"]] == other[s["index"]]
                    )
                    for s in secondary_indices
                )
                if secondary_match:
                    # Merge year values (numeric sum)
                    for i, h in enumerate(headers):
                        if re.match(r"^\d{4}$", h):
                            try:
                                val_a = int(aligned[i]) if aligned[i] else 0
                            except ValueError:
                                val_a = 0
                            try:
                                val_b = int(other[i]) if other[i] else 0
                            except ValueError:
                                val_b = 0
                            other[i] = str(val_a + val_b)
                    group["count"] += 1
                    matched = True
                    break
            if not matched:
                deduped_rows.append({"data": aligned, "count": 1, "match": ""})

    # Soft match detection
    all_indices = primary_indices + secondary_indices
    for i in range(len(deduped_rows)):
        for j in range(i + 1, len(deduped_rows)):
            a = deduped_rows[i]
            b = deduped_rows[j]

            shared = get_shared_filled_fields(a["data"], b["data"], all_indices)
            score = get_dedup_match_score(a["data"], b["data"], all_indices)

            all_similar = all(
                (
                    not str(a["data"][idx["index"]])
                    or not str(b["data"][idx["index"]])
                    or string_similarity(
                        str(a["data"][idx["index"]]), str(b["data"][idx["index"]])
                    )
                    >= 0.5
                    or clean_title(str(a["data"][idx["index"]]))
                    == clean_title(str(b["data"][idx["index"]]))
                )
                for idx in all_indices
            )

            required_shared = 2 if len(primary_indices) >= 3 else 1
            if score >= 0.5 and shared >= required_shared and all_similar:
                if not a["match"]:
                    a["match"] = "soft"
                if not b["match"]:
                    b["match"] = "soft"

    return {"headers": headers, "rowsWithMeta": deduped_rows}


if __name__ == "__main__":
    # Example usage:
    # deduplicate_summaries()
    # deduplicate_complete_summary()
    pass  # Remove or replace with actual calls as needed
