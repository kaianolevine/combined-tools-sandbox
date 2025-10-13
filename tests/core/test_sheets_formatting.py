import pytest
from unittest.mock import MagicMock, patch
from core import sheets_formatting


@pytest.fixture
def mock_service():
    return MagicMock()


def test_set_values(mock_service):
    sheets_formatting.set_values(mock_service, "sheet123", "Sheet1", 1, 1, [["a", "b"]])
    mock_service.spreadsheets().values().update.assert_called_once()


def test_set_bold_font(mock_service):
    sheets_formatting.set_bold_font(mock_service, "sheet123", 1, 2, 1, 3, 4)
    mock_service.spreadsheets().batchUpdate.assert_called_once()


def test_freeze_rows(mock_service):
    sheets_formatting.freeze_rows(mock_service, "sheet123", 1, 2)
    mock_service.spreadsheets().batchUpdate.assert_called_once()


def test_set_horizontal_alignment(mock_service):
    sheets_formatting.set_horizontal_alignment(mock_service, "sheet123", 1, 1, 2, 1, 3, "CENTER")
    mock_service.spreadsheets().batchUpdate.assert_called_once()


def test_set_number_format(mock_service):
    sheets_formatting.set_number_format(mock_service, "sheet123", 1, 2, 5, 1, 3, "TEXT")
    mock_service.spreadsheets().batchUpdate.assert_called_once()


@pytest.mark.parametrize(
    "start_col,end_col",
    [
        (1, None),
        (3, 5),
        (5, 3),  # reversed
        (-1, 2),  # clamped
    ],
)
def test_auto_resize_columns_normalized(mock_service, start_col, end_col):
    with patch("core.sheets_formatting.log") as mock_log:
        sheets_formatting.auto_resize_columns(mock_service, "sheet123", 1, start_col, end_col)
        assert mock_service.spreadsheets().batchUpdate.called
        if (
            start_col < 1
            or (end_col is not None and end_col < 1)
            or (end_col is not None and end_col < start_col)
        ):
            assert mock_log.warning.called


def test_auto_resize_columns_error_raises():
    from googleapiclient.errors import HttpError

    class MockResp:
        status = 500
        reason = "Internal Server Error"

    mock_service = MagicMock()
    mock_service.spreadsheets().batchUpdate.side_effect = HttpError(
        resp=MockResp(), content=b"fail"
    )
    with pytest.raises(sheets_formatting.HttpError):
        sheets_formatting.auto_resize_columns(mock_service, "sheet123", 1, 1, 2)


def test_update_sheet_values(mock_service):
    sheets_formatting.update_sheet_values(mock_service, "sheet123", "Sheet1", [["a", "b"]])
    mock_service.spreadsheets().values().update.assert_called_once()


def test_apply_formatting_to_sheet_empty():
    mock_gc = MagicMock()
    mock_sheet = MagicMock()
    mock_sheet.get_all_values.return_value = [[]]
    mock_gc.open_by_key.return_value.sheet1 = mock_sheet

    with patch("core.sheets_formatting.google_sheets.get_gspread_client", return_value=mock_gc):
        sheets_formatting.apply_formatting_to_sheet("sheet123")
        mock_sheet.format.assert_not_called()


def test_apply_formatting_to_sheet_success():
    mock_gc = MagicMock()
    mock_sheet = MagicMock()
    mock_sheet.get_all_values.return_value = [["Header", "Row"]]
    mock_gc.open_by_key.return_value.sheet1 = mock_sheet

    with patch("core.sheets_formatting.google_sheets.get_gspread_client", return_value=mock_gc):
        sheets_formatting.apply_formatting_to_sheet("sheet123")
        mock_sheet.format.assert_any_call("1:1", {"textFormat": {"bold": True}})
        mock_sheet.spreadsheet.batch_update.assert_called()


def test_set_column_formatting_success(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "Sheet1", "sheetId": 5}}]
    }
    sheets_formatting.set_column_formatting(mock_service, "sheet123", "Sheet1", 3)
    mock_service.spreadsheets().batchUpdate.assert_called_once()


def test_set_column_formatting_missing_sheet(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {"sheets": []}
    with patch("core.sheets_formatting.log") as mock_log:
        sheets_formatting.set_column_formatting(mock_service, "sheet123", "MissingSheet", 3)
        mock_log.warning.assert_called()
        mock_service.spreadsheets().batchUpdate.assert_not_called()


def test_set_sheet_formatting_success():
    with (
        patch("core.sheets_formatting.google_sheets.get_sheets_service") as gs_mock,
        patch("tools.dj_set_processor.helpers.hex_to_rgb", return_value={"red": 1}),
    ):
        service = gs_mock.return_value
        sheets_formatting.set_sheet_formatting(
            "sheet123", 1, 1, 3, 2, [["", ""], ["#fff", "#000"], ["#aaa", "#bbb"]]
        )
        service.spreadsheets().batchUpdate.assert_called()


def test_reorder_sheets_success(mock_service):
    metadata = {
        "sheets": [
            {"properties": {"title": "A", "sheetId": 1}},
            {"properties": {"title": "B", "sheetId": 2}},
            {"properties": {"title": "C", "sheetId": 3}},
        ]
    }
    sheets_formatting.reorder_sheets(mock_service, "sheet123", ["C", "A"], metadata)
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_format_summary_sheet_success(mock_service):
    with patch("core.sheets_formatting.google_sheets.get_sheet_id_by_name", return_value=9):
        sheets_formatting.format_summary_sheet(
            mock_service,
            "sheet123",
            "Summary",
            header=["A", "B", "C"],
            rows=[["1", "2", "3"], ["4", "5", "6"]],
        )
        mock_service.spreadsheets().batchUpdate.assert_called()
