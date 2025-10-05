import time
import re
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from difflib import SequenceMatcher
from collections import OrderedDict
import google_api

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DJ_SETS_FOLDER_ID = "1A0tKQ2DBXI1Bt9h--olFwnBNne3am-rL"
SUMMARY_FOLDER_NAME = "Summary"
ALLOWED_HEADERS = ["title", "artist", "remix", "comment", "genre", "length", "bpm", "year"]

# Constants
SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = "credentials.json"
DJ_SETS = DJ_SETS_FOLDER_ID
SUMMARY_FOLDER_NAME = "Summary"
LOCK_FILE_NAME = ".lock_summary_folder"

# Initialize Google API clients
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build("drive", "v3", credentials=credentials)
sheets_service = build("sheets", "v4", credentials=credentials)


def generate_next_missing_summary():
    logging.info("🚀 Starting generate_next_missing_summary()")
    drive_service = google_api.get_drive_service()

    summary_folder = google_api.create_folder_if_missing(
        drive_service, DJ_SETS_FOLDER_ID, SUMMARY_FOLDER_NAME
    )

    if not try_lock_folder(SUMMARY_FOLDER_NAME):
        logging.info("🔒 Summary generation is locked. Skipping run.")
        return

    year_folders = google_api.get_files_in_folder(
        drive_service, DJ_SETS_FOLDER_ID, mime_type="application/vnd.google-apps.folder"
    )
    for folder in year_folders:
        year = folder["name"]
        if year.lower() == "summary":
            continue

        summary_name = f"{year} Summary"
        existing_summaries = google_api.get_files_in_folder(
            drive_service, summary_folder["id"], name_contains=summary_name
        )
        if existing_summaries:
            logging.info(f"✅ Summary already exists for {year}")
            continue

        files = google_api.get_files_in_folder(
            drive_service, folder["id"], mime_type="application/vnd.google-apps.spreadsheet"
        )
        if any(f["name"].startswith("FAILED_") or "_Cleaned" in f["name"] for f in files):
            logging.info(f"⛔ Skipping year {year} — unready files found")
            continue

        if not try_lock_folder(year):
            logging.info(f"🔒 Year {year} is locked. Skipping.")
            continue

        try:
            logging.info(f"🔧 Generating summary for {year}...")
            generate_summary_for_folder(
                drive_service, files, summary_folder["id"], summary_name, year
            )
        finally:
            release_folder_lock(year)
            release_folder_lock(SUMMARY_FOLDER_NAME)
        break  # Only generate one per run


def generate_summary_for_folder(drive_service, files, summary_folder_id, summary_name, year):
    all_headers = set()
    sheet_data = []

    for f in files:
        logging.info(f"🔍 Reading {f['name']}")
        sheets = google_api.get_sheet_data(f["id"])  # Should return list of (header, rows)
        for header, rows in sheets:
            lower_header = [h.strip().lower() for h in header]
            keep_indices = [i for i, h in enumerate(lower_header) if h in ALLOWED_HEADERS]
            if not keep_indices:
                continue
            filtered_header = [header[i] for i in keep_indices]
            filtered_rows = [
                [row[i] for i in keep_indices] for row in rows if any(cell.strip() for cell in row)
            ]
            if filtered_rows:
                all_headers.update(filtered_header)
                sheet_data.append((filtered_header, filtered_rows))

    if not sheet_data:
        logging.info(f"📭 No valid data found in folder: {year}")
        return

    final_header = list(all_headers) + ["Count"]
    final_rows = []
    for header, rows in sheet_data:
        idx_map = {h: i for i, h in enumerate(header)}
        for row in rows:
            aligned = [row[idx_map[h]] if h in idx_map else "" for h in final_header[:-1]]
            final_rows.append(aligned + [1])

    if "Title" in final_header:
        title_index = final_header.index("Title")
        final_rows.sort(key=lambda r: r[title_index])
    else:
        final_rows.sort()

    ss_id = create_spreadsheet(summary_name)
    google_api.write_sheet_data(ss_id, "Summary", final_header, final_rows)
    google_api.format_summary_sheet(ss_id, "Summary", final_header, final_rows)

    move_file_to_folder(drive_service, ss_id, summary_folder_id)
    logging.info(f"✅ Summary successfully created: {summary_name}")


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
            # signature = "|".join(aligned[p["index"]] for p in primary_indices)

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


def authenticate():
    """
    Authenticates and returns the Google Drive and Sheets service clients.
    """
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    return drive_service, sheets_service


def get_or_create_subfolder(parent_folder_id, subfolder_name):
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
    logger.info(
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
            logger.info(f"📄 Found existing file '{name}' with ID {files[0]['id']}")
            return files[0]["id"]
        else:
            logger.info(
                f"➕ No existing file named '{name}' — creating new one in parent {parent_folder_id}"
            )
            file_metadata = {"name": name, "mimeType": mime_type, "parents": [parent_folder_id]}
            file = (
                drive_service.files()
                .create(body=file_metadata, fields="id", supportsAllDrives=True)
                .execute()
            )
            logger.info(f"🆕 Created new file '{name}' with ID {file['id']}")
            return file["id"]
    except HttpError as error:
        logger.error(f"An error occurred while finding or creating file: {error}")
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


def delete_all_sheets_except(sheets_service, spreadsheet_id, sheet_to_keep):
    """
    Deletes all sheets except the one named sheet_to_keep.
    """
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet.get("sheets", [])
    requests = []
    for sheet in sheets:
        title = sheet["properties"]["title"]
        sheet_id = sheet["properties"]["sheetId"]
        if title != sheet_to_keep:
            requests.append({"deleteSheet": {"sheetId": sheet_id}})
    if requests:
        body = {"requests": requests}
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body=body
        ).execute()


def set_values(sheets_service, spreadsheet_id, sheet_name, start_row, start_col, values):
    """
    Sets values in a sheet starting at (start_row, start_col).
    """
    end_row = start_row + len(values) - 1
    end_col = start_col + len(values[0]) - 1 if values else start_col
    range_name = f"{sheet_name}!R{start_row}C{start_col}:R{end_row}C{end_col}"
    body = {"values": values}
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name, valueInputOption="RAW", body=body
    ).execute()


def set_bold_font(
    sheets_service, spreadsheet_id, sheet_id, start_row, end_row, start_col, end_col
):
    """
    Sets font weight to bold for the specified range.
    """
    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row - 1,
                    "endRowIndex": end_row,
                    "startColumnIndex": start_col - 1,
                    "endColumnIndex": end_col,
                },
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        }
    ]
    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def freeze_rows(sheets_service, spreadsheet_id, sheet_id, num_rows):
    """
    Freezes the specified number of rows at the top of the sheet.
    """
    requests = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": num_rows},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        }
    ]
    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def set_horizontal_alignment(
    sheets_service,
    spreadsheet_id,
    sheet_id,
    start_row,
    end_row,
    start_col,
    end_col,
    alignment="LEFT",
):
    """
    Sets horizontal alignment for a range.
    """
    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row - 1,
                    "endRowIndex": end_row,
                    "startColumnIndex": start_col - 1,
                    "endColumnIndex": end_col,
                },
                "cell": {"userEnteredFormat": {"horizontalAlignment": alignment}},
                "fields": "userEnteredFormat.horizontalAlignment",
            }
        }
    ]
    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def set_number_format(
    sheets_service, spreadsheet_id, sheet_id, start_row, end_row, start_col, end_col, format_str
):
    """
    Sets number format for a range.
    """
    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row - 1,
                    "endRowIndex": end_row,
                    "startColumnIndex": start_col - 1,
                    "endColumnIndex": end_col,
                },
                "cell": {
                    "userEnteredFormat": {"numberFormat": {"type": "TEXT", "pattern": format_str}}
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        }
    ]
    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def auto_resize_columns(sheets_service, spreadsheet_id, sheet_id, start_col, end_col):
    """
    Auto-resizes columns from start_col to end_col.
    """
    requests = [
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": start_col - 1,
                    "endIndex": end_col,
                }
            }
        }
    ]
    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def generate_complete_summary():
    """
    Generates a consolidated 'Complete Summary' spreadsheet from yearly summary spreadsheets.
    """
    try:
        drive_service, sheets_service = authenticate()
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        return

    # Step 0: Get or create 'Summary' folder inside parent folder DJ_SETS
    try:
        summary_folder_id = get_or_create_subfolder(DJ_SETS, "Summary")
    except HttpError as e:
        logger.error(f"Failed to get or create 'Summary' folder: {e}")
        return

    # List all files in summary folder
    try:
        summary_files = list_files_in_folder(drive_service, summary_folder_id)
    except HttpError as e:
        logger.error(f"Failed to list files in Summary folder: {e}")
        return

    # Determine output spreadsheet name
    output_name = "_pending_Complete Summary"
    # Check if any file with name containing "complete summary" exists (case insensitive)
    for f in summary_files:
        if (
            f["mimeType"] == "application/vnd.google-apps.spreadsheet"
            and "complete summary" in f["name"].lower()
        ):
            logger.warning(f'⚠️ Existing "Complete Summary" detected — using name: {output_name}')
            break

    # Check if output file already exists
    existing_file = get_file_by_name(drive_service, summary_folder_id, output_name)
    if existing_file:
        master_file_id = existing_file["id"]
        logger.info(f'Using existing master file "{output_name}" with ID: {master_file_id}')
    else:
        # Create new spreadsheet
        master_file_id = create_spreadsheet(drive_service, output_name, summary_folder_id)

    # Open master spreadsheet info
    try:
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=master_file_id).execute()
    except HttpError as e:
        logger.error(f"Failed to open master spreadsheet: {e}")
        return

    # Delete all sheets except "Sheet1"
    delete_all_sheets_except(sheets_service, master_file_id, "Sheet1")

    # Clear "Sheet1"
    clear_sheet(sheets_service, master_file_id, "Sheet1")

    # Step 1: Gather and normalize all rows from summary files
    summary_data = []
    year_set = set()

    # We only want files named like "YYYY Summary"
    year_summary_pattern = re.compile(r"^(\d{4}) Summary$")

    for file in summary_files:
        file_name = file["name"].strip()
        match = year_summary_pattern.match(file_name)
        if not match:
            continue
        year = match.group(1)
        year_set.add(year)
        file_id = file["id"]

        try:
            # Get first sheet name
            file_spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=file_id).execute()
            sheets_list = file_spreadsheet.get("sheets", [])
            if not sheets_list:
                logger.warning(f'File "{file_name}" has no sheets, skipping.')
                continue
            source_sheet_title = sheets_list[0]["properties"]["title"]

            data = get_sheet_values(sheets_service, file_id, source_sheet_title)
            if len(data) < 2:
                continue

            headers = [str(h).strip() for h in data[0]]
            lower_headers = [h.lower() for h in headers]
            try:
                count_index = lower_headers.index("count")
            except ValueError:
                count_index = -1

            rows = data[1:]
            for row in rows:
                count = 1
                if count_index >= 0 and count_index < len(row):
                    try:
                        count = int(row[count_index])
                    except (ValueError, TypeError):
                        count = 1
                summary_data.append({"year": year, "headers": headers, "row": row, "count": count})
        except HttpError as e:
            logger.error(f'❌ Failed to read "{file_name}": {e}')

    if not summary_data:
        logger.warning("⚠️ No summary files found. Created empty Complete Summary.")
        return

    # Step 2: Build a unified header set using lowercase deduplication
    header_map = OrderedDict()  # lowercased => original casing
    for entry in summary_data:
        for h in entry["headers"]:
            key = h.lower()
            if key != "count" and key not in header_map:
                header_map[key] = h

    base_headers = list(header_map.values())
    years = sorted(year_set)
    final_headers = base_headers + years

    # Step 3: Consolidate rows using stringified deduplication keys
    row_map = dict()

    for entry in summary_data:
        year = entry["year"]
        headers = entry["headers"]
        row = entry["row"]
        count = entry["count"]

        normalized_row = {}
        for i, h in enumerate(headers):
            key = h.lower()
            if key != "count" and key in header_map:
                normalized_row[header_map[key]] = str(row[i]) if i < len(row) else ""

        signature = "|".join(normalized_row.get(h, "") for h in base_headers)

        if signature not in row_map:
            year_data = {y: "" for y in years}
            row_map[signature] = {**normalized_row, **year_data}

        existing_count = row_map[signature].get(year, "")
        try:
            existing_count_int = int(existing_count) if existing_count else 0
        except ValueError:
            existing_count_int = 0
        row_map[signature][year] = str(existing_count_int + count)

    # Step 4: Write to sheet
    final_rows = [final_headers]
    for row_obj in row_map.values():
        final_rows.append([row_obj.get(h, "") for h in final_headers])

    # Write values to sheet
    set_values(sheets_service, master_file_id, "Sheet1", 1, 1, final_rows)

    # Get sheet ID of "Sheet1"
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=master_file_id).execute()
    sheet_id = None
    for sheet in spreadsheet.get("sheets", []):
        if sheet["properties"]["title"] == "Sheet1":
            sheet_id = sheet["properties"]["sheetId"]
            break
    if sheet_id is None:
        logger.error('Sheet "Sheet1" not found in master spreadsheet.')
        return

    # Set header row bold
    set_bold_font(sheets_service, master_file_id, sheet_id, 1, 1, 1, len(final_headers))

    # Freeze header row
    freeze_rows(sheets_service, master_file_id, sheet_id, 1)

    # Set horizontal alignment to left for all data
    set_horizontal_alignment(
        sheets_service, master_file_id, sheet_id, 1, len(final_rows), 1, len(final_headers), "LEFT"
    )

    # Set number format to text for data rows (excluding header)
    if len(final_rows) > 1:
        set_number_format(
            sheets_service,
            master_file_id,
            sheet_id,
            2,
            len(final_rows),
            1,
            len(final_headers),
            "@STRING@",
        )

    # Auto resize columns and adjust width with max 200 pixels
    # Google Sheets API does not support setting column width directly with batchUpdate.
    # We can only auto resize columns.
    auto_resize_columns(sheets_service, master_file_id, sheet_id, 1, len(final_headers))

    logger.info(f"✅ {output_name} generated.")


if __name__ == "__main__":
    generate_complete_summary()
