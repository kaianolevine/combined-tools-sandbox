import pytest
from unittest.mock import MagicMock, patch
from core import google_sheets


@pytest.fixture
def mock_service():
    return MagicMock()


def test_get_or_create_sheet_creates_new(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {"sheets": []}
    google_sheets.get_or_create_sheet(mock_service, "sheet123", "NewTab")
    mock_service.spreadsheets().batchUpdate.assert_called_once()


def test_get_or_create_sheet_already_exists(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "Existing"}}]
    }
    google_sheets.get_or_create_sheet(mock_service, "sheet123", "Existing")
    mock_service.spreadsheets().batchUpdate.assert_not_called()


def test_read_sheet(mock_service):
    mock_service.spreadsheets().values().get().execute.return_value = {"values": [["a", "b"]]}
    result = google_sheets.read_sheet(mock_service, "sheet123", "Sheet1!A1:B1")
    assert result == [["a", "b"]]


def test_write_sheet(mock_service):
    mock_service.spreadsheets().values().update().execute.return_value = {"updatedCells": 2}
    result = google_sheets.write_sheet(mock_service, "sheet123", "Sheet1!A1", [["X", "Y"]])
    assert result["updatedCells"] == 2


def test_append_rows(mock_service):
    mock_service.spreadsheets().values().append().execute.return_value = {"updates": {}}
    google_sheets.append_rows(mock_service, "sheet123", "Sheet1!A1", [["1", "2"]])


def test_log_info_sheet(mock_service):
    with patch("core.google_sheets.append_rows") as mock_append:
        google_sheets.log_info_sheet(mock_service, "sheet123", "Test message")
        mock_append.assert_called_once()


def test_ensure_sheet_exists_writes_headers(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {"sheets": []}
    mock_service.spreadsheets().values().get().execute.return_value = {}
    google_sheets.ensure_sheet_exists(mock_service, "sheet123", "Tab", ["A", "B"])
    mock_service.spreadsheets().values().update.assert_called()


def test_get_sheet_metadata(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {"spreadsheetId": "sheet123"}
    result = google_sheets.get_sheet_metadata(mock_service, "sheet123")
    assert result["spreadsheetId"] == "sheet123"


def test_update_row(monkeypatch):
    fake_service = MagicMock()
    monkeypatch.setattr(google_sheets, "get_sheets_service", lambda: fake_service)
    google_sheets.update_row("sheet123", "Sheet1!A1", [["a", "b", "c"]])
    fake_service.spreadsheets().values().update.assert_called()


def test_sort_sheet_by_column(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "Data", "sheetId": 99}}]
    }
    google_sheets.sort_sheet_by_column(mock_service, "sheet123", "Data", 1)
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_get_sheet_id_by_name(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "MySheet", "sheetId": 321}}]
    }
    assert google_sheets.get_sheet_id_by_name(mock_service, "sheet123", "MySheet") == 321


def test_rename_sheet(mock_service):
    google_sheets.rename_sheet(mock_service, "sheet123", 123, "Renamed")
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_insert_rows(mock_service):
    google_sheets.insert_rows(mock_service, "sheet123", "Sheet1", [["hello", "world"]])
    mock_service.spreadsheets().values().update.assert_called()


def test_get_sheet_values(mock_service):
    mock_service.spreadsheets().values().get().execute.return_value = {"values": [[1, None, "X"]]}
    result = google_sheets.get_sheet_values(mock_service, "sheet123", "Data")
    assert result == [["1", "", "X"]]


def test_clear_all_except_one_sheet(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [
            {"properties": {"title": "One", "sheetId": 1}},
            {"properties": {"title": "Two", "sheetId": 2}},
        ]
    }
    google_sheets.clear_all_except_one_sheet(mock_service, "sheet123", "Two")
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_clear_sheet(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "Target", "sheetId": 42}}]
    }
    google_sheets.clear_sheet(mock_service, "sheet123", "Target")
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_delete_sheet_by_name(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [
            {"properties": {"title": "ToKeep", "sheetId": 1}},
            {"properties": {"title": "ToDelete", "sheetId": 2}},
        ]
    }
    google_sheets.delete_sheet_by_name(mock_service, "sheet123", "ToDelete")
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_delete_all_sheets_except(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [
            {"properties": {"title": "Main", "sheetId": 10}},
            {"properties": {"title": "Extra", "sheetId": 11}},
        ]
    }
    google_sheets.delete_all_sheets_except(mock_service, "sheet123", "Main")
    mock_service.spreadsheets().batchUpdate.assert_called()
