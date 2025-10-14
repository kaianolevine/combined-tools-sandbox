import pytest
from unittest.mock import Mock
from googleapiclient.errors import HttpError
from core import sheets_formatting as sf


# =====================================================
# Fixtures
# =====================================================


@pytest.fixture
def mock_service():
    """A reusable fake Sheets API service."""
    service = Mock()
    service.spreadsheets.return_value = Mock()
    service.spreadsheets().batchUpdate.return_value.execute.return_value = {}
    service.spreadsheets().values.return_value = Mock()
    service.spreadsheets().values().update.return_value.execute.return_value = {}
    return service


# =====================================================
# apply_sheet_formatting
# =====================================================


def test_apply_sheet_formatting_calls_expected_methods():
    sheet = Mock()
    sheet._properties = {"sheetId": 123}
    sf.apply_sheet_formatting(sheet)
    sheet.format.assert_any_call(
        "A:Z", {"textFormat": {"fontSize": 10}, "horizontalAlignment": "LEFT"}
    )
    sheet.freeze.assert_called_once()
    sheet.spreadsheet.batch_update.assert_called_once()


# =====================================================
# apply_formatting_to_sheet
# =====================================================


def test_apply_formatting_to_sheet_success(monkeypatch):
    sheet = Mock()
    sheet.get_all_values.return_value = [["A", "B"]]
    sh = Mock(sheet1=sheet)
    gc = Mock(open_by_key=lambda k: sh)
    monkeypatch.setattr(sf.google_sheets, "get_gspread_client", lambda: gc)
    monkeypatch.setattr(sf, "apply_sheet_formatting", lambda s: setattr(s, "called", True))
    sf.apply_formatting_to_sheet("spreadsheet_id")
    assert hasattr(sheet, "called")


def test_apply_formatting_to_sheet_empty(monkeypatch):
    sheet = Mock()
    sheet.get_all_values.return_value = []
    sh = Mock(sheet1=sheet)
    gc = Mock(open_by_key=lambda k: sh)
    monkeypatch.setattr(sf.google_sheets, "get_gspread_client", lambda: gc)
    result = sf.apply_formatting_to_sheet("spreadsheet_id")
    assert result is None


def test_apply_formatting_to_sheet_exception(monkeypatch):
    monkeypatch.setattr(
        sf.google_sheets, "get_gspread_client", lambda: (_ for _ in ()).throw(Exception("fail"))
    )
    sf.apply_formatting_to_sheet("id")  # Should log error but not raise


# =====================================================
# set_values
# =====================================================


def test_set_values_updates_correct_range(mock_service):
    sf.set_values(mock_service, "id", "Sheet1", 1, 1, [["A", "B"], ["C", "D"]])
    mock_service.spreadsheets().values().update.assert_called()


# =====================================================
# set_bold_font
# =====================================================


def test_set_bold_font_calls_batch_update(mock_service):
    sf.set_bold_font(mock_service, "id", 1, 1, 2, 1, 3)
    mock_service.spreadsheets().batchUpdate.assert_called()


# =====================================================
# freeze_rows
# =====================================================


def test_freeze_rows_calls_batch_update(mock_service):
    sf.freeze_rows(mock_service, "id", 1, 3)
    mock_service.spreadsheets().batchUpdate.assert_called()


# =====================================================
# set_horizontal_alignment
# =====================================================


def test_set_horizontal_alignment_default(mock_service):
    sf.set_horizontal_alignment(mock_service, "id", 1, 1, 5, 1, 3)
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_set_horizontal_alignment_custom(mock_service):
    sf.set_horizontal_alignment(mock_service, "id", 1, 1, 5, 1, 3, alignment="RIGHT")
    mock_service.spreadsheets().batchUpdate.assert_called()


# =====================================================
# set_number_format
# =====================================================


def test_set_number_format_applies_format(mock_service):
    sf.set_number_format(mock_service, "id", 1, 1, 5, 1, 3, "TEXT")
    mock_service.spreadsheets().batchUpdate.assert_called()


# =====================================================
# auto_resize_columns
# =====================================================


def test_auto_resize_columns_valid(mock_service):
    sf.auto_resize_columns(mock_service, "id", 1, 1, 5)
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_auto_resize_columns_swapped(monkeypatch, mock_service):
    monkeypatch.setattr(sf.log, "warning", lambda m: None)
    sf.auto_resize_columns(mock_service, "id", 1, 5, 2)
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_auto_resize_columns_handles_HttpError(monkeypatch, mock_service):
    mock_service.spreadsheets().batchUpdate.side_effect = HttpError(
        resp=Mock(status=400), content=b"fail"
    )
    monkeypatch.setattr(sf.log, "error", lambda m: None)
    with pytest.raises(HttpError):
        sf.auto_resize_columns(mock_service, "id", 1, 1, 2)


# =====================================================
# update_sheet_values
# =====================================================


def test_update_sheet_values(mock_service):
    sf.update_sheet_values(mock_service, "id", "Sheet1", [["x"]])
    mock_service.spreadsheets().values().update.assert_called()


# =====================================================
# set_sheet_formatting
# =====================================================


def test_set_sheet_formatting_builds_requests(monkeypatch, mock_service):
    monkeypatch.setattr(sf.google_sheets, "get_sheets_service", lambda: mock_service)
    monkeypatch.setattr(sf.helpers, "hex_to_rgb", lambda c: {"red": 1, "green": 1, "blue": 1})
    sf.set_sheet_formatting(mock_service, "id", 1, 2, 3, [["#FFFFFF"], ["#000000"]])
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_set_sheet_formatting_no_backgrounds(monkeypatch, mock_service):
    monkeypatch.setattr(sf.google_sheets, "get_sheets_service", lambda: mock_service)
    sf.set_sheet_formatting(mock_service, "id", 1, 2, 3, [["#FFFFFF"]])
    mock_service.spreadsheets().batchUpdate.assert_called()


# =====================================================
# set_column_formatting
# =====================================================


def test_set_column_formatting_success(monkeypatch, mock_service):
    mock_service.spreadsheets().get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "Sheet1", "sheetId": 123}}]
    }
    sf.set_column_formatting(mock_service, "id", "Sheet1", 3)
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_set_column_formatting_missing_sheet(monkeypatch, mock_service):
    mock_service.spreadsheets().get.return_value.execute.return_value = {"sheets": []}
    monkeypatch.setattr(sf.log, "warning", lambda m: None)
    sf.set_column_formatting(mock_service, "id", "Sheet1", 3)
    mock_service.spreadsheets().batchUpdate.assert_not_called()


def test_set_column_formatting_http_error(monkeypatch, mock_service):
    mock_service.spreadsheets().get.side_effect = HttpError(resp=Mock(status=400), content=b"fail")
    monkeypatch.setattr(sf.log, "error", lambda m: None)
    with pytest.raises(HttpError):
        sf.set_column_formatting(mock_service, "id", "Sheet1", 3)


# =====================================================
# reorder_sheets
# =====================================================


def test_reorder_sheets_success(monkeypatch, mock_service):
    metadata = {
        "sheets": [
            {"properties": {"title": "A", "sheetId": 1}},
            {"properties": {"title": "B", "sheetId": 2}},
        ]
    }
    monkeypatch.setattr(sf.log, "info", lambda m: None)
    sf.reorder_sheets(mock_service, "id", ["B"], metadata)
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_reorder_sheets_http_error(monkeypatch, mock_service):
    mock_service.spreadsheets().batchUpdate.side_effect = HttpError(
        resp=Mock(status=400), content=b"fail"
    )
    monkeypatch.setattr(sf.log, "error", lambda m: None)
    metadata = {"sheets": [{"properties": {"title": "A", "sheetId": 1}}]}
    with pytest.raises(HttpError):
        sf.reorder_sheets(mock_service, "id", ["A"], metadata)


# =====================================================
# format_summary_sheet
# =====================================================


def test_format_summary_sheet_calls_batch_update(monkeypatch, mock_service):
    monkeypatch.setattr(sf.google_sheets, "get_sheet_id_by_name", lambda s, i, n: 1)
    sf.format_summary_sheet(mock_service, "id", "Sheet1", ["Col1", "Col2"], [["a", "b"]])
    mock_service.spreadsheets().batchUpdate.assert_called()
