from typing import Any, List, Dict
import tools.dj_set_processor.helpers as helpers
from core import logger as log
from googleapiclient.errors import HttpError
from core import google_sheets


def apply_sheet_formatting(sheet):
    # Set font size and alignment for entire sheet
    sheet.format("A:Z", {"textFormat": {"fontSize": 10}, "horizontalAlignment": "LEFT"})

    # Freeze header row
    sheet.freeze(rows=1)

    # Bold the header row
    sheet.format("1:1", {"textFormat": {"bold": True}})

    # Auto resize first 10 columns (A-J)
    sheet.spreadsheet.batch_update(
        {
            "requests": [
                {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet._properties["sheetId"],
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": 10,
                        }
                    }
                }
            ]
        }
    )


def apply_formatting_to_sheet(spreadsheet_id):
    log.debug(f"Applying formatting to spreadsheet ID: {spreadsheet_id}")
    try:
        gc = google_sheets.get_gspread_client()
        sh = gc.open_by_key(spreadsheet_id)
        sheet = sh.sheet1
        log.debug("Opened sheet for formatting")

        # Get all values to determine range size
        values = sheet.get_all_values()
        if not values or len(values) == 0 or len(values[0]) == 0:
            log.warning("Sheet is empty, skipping formatting")
            return

        apply_sheet_formatting(sheet)

        log.info("✅ Formatting applied successfully")
    except Exception as e:
        log.error(f"Error applying formatting: {e}")


def set_values(sheets_service, spreadsheet_id, sheet_name, start_row, start_col, values):
    """
    Sets values in a sheet starting at (start_row, start_col).
    """
    end_row = start_row + len(values) - 1
    end_col = start_col + len(values[0]) - 1 if values else start_col
    range_name = f"{sheet_name}!R{start_row}C{start_col}:R{end_row}C{end_col}"
    body = {"values": [[f"'{str(cell)}" for cell in row] for row in values]}
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
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "TEXT"}}},
                "fields": "userEnteredFormat.numberFormat",
            }
        }
    ]
    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def auto_resize_columns(
    service, spreadsheet_id, sheet_id, start_col: int = 1, end_col: int | None = None
):
    """Automatically resize columns in a given sheet range.

    Args:
        service: Google Sheets API service instance.
        spreadsheet_id: ID of the spreadsheet.
        sheet_id: Sheet ID within the spreadsheet.
        start_col: 1-based first column to resize (default 1).
        end_col: 1-based last column to resize (inclusive). If None, will resize just start_col.

    This function is defensive: it validates and normalizes the start/end column values,
    converts them to the zero-based, end-exclusive indices expected by the Sheets API,
    and swaps the bounds if they were passed in the wrong order. If the API call fails
    with an HttpError, it will log the error and re-raise.
    """
    try:
        # Normalize inputs to integers and ensure at least 1
        start_col = int(start_col) if start_col is not None else 1
        if start_col < 1:
            log.warning(f"start_col < 1 ({start_col}) — clamping to 1")
            start_col = 1

        if end_col is None:
            end_col = start_col
        else:
            end_col = int(end_col)
            if end_col < 1:
                log.warning(f"end_col < 1 ({end_col}) — clamping to 1")
                end_col = 1

        # If bounds are reversed, swap them (caller may have passed in reversed args)
        if end_col < start_col:
            log.warning(
                f"auto_resize_columns received end_col < start_col ({end_col} < {start_col}); swapping bounds"
            )
            start_col, end_col = end_col, start_col

        # Convert to zero-based, end-exclusive indices for Sheets API
        start_index = start_col - 1
        end_index = end_col  # endIndex in the API is exclusive and zero-based

        body = {
            "requests": [
                {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": start_index,
                            "endIndex": end_index,
                        }
                    }
                }
            ]
        }

        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    except HttpError as e:
        log.error(
            f"Auto-resize columns failed for sheet {sheet_id} cols {start_col}-{end_col}: {e}"
        )
        raise


def update_sheet_values(sheets_service, spreadsheet_id, sheet_name, values):
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
    Apply formatting to a Google Sheet:
    - Freeze header rows
    - Set font bold for header row
    - Set horizontal alignment left for all data
    - Set number format to plain text for data rows
    - Set background colors for data rows
    - Auto resize columns
    """
    sheets_service = google_sheets.get_sheets_service()
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
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "TEXT"}}},
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
                                {
                                    "userEnteredFormat": {
                                        "backgroundColor": helpers.hex_to_rgb(color)
                                    }
                                }
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

    # Auto resize columns
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
    # Can't set max width directly via API; auto-resize only.

    body = {"requests": requests}
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def set_column_formatting(sheets_service, spreadsheet_id: str, sheet_name: str, num_columns: int):
    """
    Sets formatting for specified columns (first column date, others text).
    """
    log.info(f"🎨 Setting column formatting for {num_columns} columns in sheet '{sheet_name}'")
    try:
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = None
        for sheet in spreadsheet.get("sheets", []):
            if sheet["properties"]["title"] == sheet_name:
                sheet_id = sheet["properties"]["sheetId"]
                break
        if sheet_id is None:
            log.warning(f"Sheet '{sheet_name}' not found for formatting")
            return

        requests = []
        # Format first column as DATE
        if num_columns >= 1:
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 1000000,
                            "startColumnIndex": 0,
                            "endColumnIndex": 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}
                            }
                        },
                        "fields": "userEnteredFormat.numberFormat",
                    }
                }
            )
        # Format other columns as TEXT
        if num_columns > 1:
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 1000000,
                            "startColumnIndex": 1,
                            "endColumnIndex": num_columns,
                        },
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "TEXT"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                }
            )

        if requests:
            body = {"requests": requests}
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body
            ).execute()
            log.info("✅ Column formatting set successfully")
    except HttpError as error:
        log.error(f"An error occurred while setting column formatting: {error}")
        raise


def reorder_sheets(
    sheets_service,
    spreadsheet_id: str,
    sheet_names_in_order: List[str],
    spreadsheet_metadata: Dict,
):
    """
    Reorders sheets in the spreadsheet to match the order of sheet_names_in_order.
    Sheets not in the list will be placed after those specified.
    """
    log.info(
        f"🔀 Reordering sheets in spreadsheet ID {spreadsheet_id} to order: {sheet_names_in_order}"
    )
    try:
        sheets = spreadsheet_metadata.get("sheets", [])
        title_to_id = {
            sheet["properties"]["title"]: sheet["properties"]["sheetId"] for sheet in sheets
        }
        requests = []
        index = 0
        for name in sheet_names_in_order:
            sheet_id = title_to_id.get(name)
            if sheet_id is not None:
                requests.append(
                    {
                        "updateSheetProperties": {
                            "properties": {"sheetId": sheet_id, "index": index},
                            "fields": "index",
                        }
                    }
                )
                index += 1
        remaining_sheets = [
            sheet for sheet in sheets if sheet["properties"]["title"] not in sheet_names_in_order
        ]
        for sheet in remaining_sheets:
            requests.append(
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": sheet["properties"]["sheetId"], "index": index},
                        "fields": "index",
                    }
                }
            )
            index += 1
        if requests:
            body = {"requests": requests}
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body
            ).execute()
            log.info("✅ Sheets reordered successfully")
    except HttpError as error:
        log.error(f"An error occurred while reordering sheets: {error}")
        raise


def format_summary_sheet(
    sheet_service, spreadsheet_id: str, sheet_name: str, header: List[str], rows: List[List[Any]]
) -> None:
    """
    Applies formatting to a summary sheet, such as:
    - Bold header row
    - Frozen header
    - Auto-sized columns (limited width)
    - Plain text formatting
    - Gridlines and optional visual enhancements

    Args:
        sheet_service: The Google Sheets API service instance.
        spreadsheet_id (str): The ID of the spreadsheet.
        sheet_name (str): The sheet name to format.
        header (List[str]): The list of column headers (used for column count).
        rows (List[List[Any]]): The data rows (used for row count).
    """
    sheet_id = google_sheets.get_sheet_id_by_name(sheet_service, spreadsheet_id, sheet_name)
    requests = []

    num_columns = len(header)
    num_rows = len(rows) + 1  # +1 for header

    # Freeze header row
    requests.append(
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        }
    )

    # Bold the header row
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                },
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        }
    )

    # Auto resize columns (limited width)
    requests.append(
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": num_columns,
                }
            }
        }
    )

    # Format all cells as plain text
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": num_rows,
                    "startColumnIndex": 0,
                    "endColumnIndex": num_columns,
                },
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "TEXT"}}},
                "fields": "userEnteredFormat.numberFormat",
            }
        }
    )

    # Send all formatting requests in batch
    sheet_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": requests}
    ).execute()
