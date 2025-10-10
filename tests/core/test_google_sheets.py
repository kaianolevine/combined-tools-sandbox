import pytest
from unittest import mock
from googleapiclient.errors import HttpError
from core import google_sheets


@pytest.fixture
def mock_service():
    return mock.MagicMock()


# ---- Basic Credential & Service Access ----


def test_get_sheets_service(monkeypatch):
    fake_client = mock.MagicMock()
    monkeypatch.setattr(
        google_sheets._google_credentials, "get_sheets_client", lambda: fake_client
    )
    assert google_sheets.get_sheets_service() == fake_client


def test_get_gspread_client(monkeypatch):
    fake_client = mock.MagicMock()
    monkeypatch.setattr(
        google_sheets._google_credentials, "get_gspread_client", lambda: fake_client
    )
    assert google_sheets.get_gspread_client() == fake_client


# ---- Sheet Creation / Metadata ----


def test_get_or_create_sheet_creates(monkeypatch, mock_service):
    fake_spreadsheets = mock_service.spreadsheets.return_value
    fake_spreadsheets.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "Existing"}}]
    }
    fake_spreadsheets.batchUpdate.return_value.execute.return_value = {"done": True}
    monkeypatch.setattr(google_sheets, "get_sheets_service", lambda: mock_service)

    google_sheets.get_or_create_sheet("spreadsheet123", "NewSheet")

    fake_spreadsheets.batchUpdate.assert_called_once()


def test_get_or_create_sheet_already_exists(monkeypatch, mock_service):
    fake_spreadsheets = mock_service.spreadsheets.return_value
    fake_spreadsheets.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "MyTab"}}]
    }
    monkeypatch.setattr(google_sheets, "get_sheets_service", lambda: mock_service)

    google_sheets.get_or_create_sheet("spreadsheet123", "MyTab")
    fake_spreadsheets.batchUpdate.assert_not_called()


def test_get_sheet_metadata(monkeypatch, mock_service):
    fake_spreadsheets = mock_service.spreadsheets.return_value
    fake_spreadsheets.get.return_value.execute.return_value = {"title": "Test"}
    monkeypatch.setattr(google_sheets, "get_sheets_service", lambda: mock_service)

    meta = google_sheets.get_sheet_metadata("sheetid")
    assert meta["title"] == "Test"


def test_get_sheet_metadata_http_error(monkeypatch, mock_service):
    fake_spreadsheets = mock_service.spreadsheets.return_value
    fake_spreadsheets.get.return_value.execute.side_effect = HttpError(mock.Mock(), b"fail")
    monkeypatch.setattr(google_sheets, "get_sheets_service", lambda: mock_service)
    with pytest.raises(HttpError):
        google_sheets.get_sheet_metadata("sheetid")


# ---- Read / Write ----


def test_read_sheet(monkeypatch, mock_service):
    fake_values = [["a", "b"], ["c", "d"]]
    mock_service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
        "values": fake_values
    }
    monkeypatch.setattr(google_sheets, "get_sheets_service", lambda: mock_service)

    result = google_sheets.read_sheet("sheetid", "Sheet1!A1:B2")
    assert result == fake_values


def test_write_sheet(monkeypatch, mock_service):
    mock_service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {
        "updatedRows": 2
    }
    monkeypatch.setattr(google_sheets, "get_sheets_service", lambda: mock_service)

    result = google_sheets.write_sheet("sid", "Sheet1!A1", [["1", "2"]])
    assert "updatedRows" in result


def test_append_rows(monkeypatch, mock_service):
    mock_service.spreadsheets.return_value.values.return_value.append.return_value.execute.return_value = {
        "updates": {}
    }
    monkeypatch.setattr(google_sheets, "get_sheets_service", lambda: mock_service)
    google_sheets.append_rows("sid", "Sheet1!A1", [["1", "2"]])
    mock_service.spreadsheets.return_value.values.return_value.append.assert_called_once()


# ---- Logging Tabs ----


def test_log_debug_and_info(monkeypatch):
    calls = []
    monkeypatch.setattr(google_sheets, "get_or_create_sheet", lambda a, b: calls.append(b))
    monkeypatch.setattr(google_sheets, "append_rows", lambda a, b, c: calls.append(b))

    google_sheets.log_debug("sheetid", "hello debug")
    google_sheets.log_info("sheetid", "hello info")

    assert "Debug!A1" in calls
    assert "Info!A1" in calls


# ---- Ensure Sheet Exists ----


def test_ensure_sheet_exists_no_headers(monkeypatch):
    monkeypatch.setattr(google_sheets, "get_or_create_sheet", lambda *a, **k: None)
    google_sheets.ensure_sheet_exists("sid", "TestTab")


def test_ensure_sheet_exists_with_new_headers(monkeypatch):
    monkeypatch.setattr(google_sheets, "get_or_create_sheet", lambda *a, **k: None)
    monkeypatch.setattr(google_sheets, "read_sheet", lambda *a, **k: [])
    monkeypatch.setattr(google_sheets, "write_sheet", lambda *a, **k: {"done": True})
    google_sheets.ensure_sheet_exists("sid", "Tab", ["A", "B", "C"])


# ---- Row Update ----


def test_update_row(monkeypatch, mock_service):
    mock_service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {
        "updated": True
    }
    monkeypatch.setattr(google_sheets, "get_sheets_service", lambda: mock_service)
    res = google_sheets.update_row("sid", "Sheet1!A2:B2", [["1", "2"]])
    assert res["updated"]


# ---- Sort Sheet ----


def test_sort_sheet_by_column(monkeypatch, mock_service):
    fake_meta = {"sheets": [{"properties": {"title": "Tab", "sheetId": 99}}]}
    monkeypatch.setattr(google_sheets, "get_sheets_service", lambda: mock_service)
    monkeypatch.setattr(google_sheets, "get_sheet_metadata", lambda sid: fake_meta)
    mock_service.spreadsheets.return_value.batchUpdate.return_value.execute.return_value = {
        "done": True
    }

    res = google_sheets.sort_sheet_by_column("sid", "Tab", 0, True)
    assert res["done"]


def test_sort_sheet_by_column_missing(monkeypatch, mock_service):
    fake_meta = {"sheets": [{"properties": {"title": "OtherTab", "sheetId": 1}}]}
    monkeypatch.setattr(google_sheets, "get_sheets_service", lambda: mock_service)
    monkeypatch.setattr(google_sheets, "get_sheet_metadata", lambda sid: fake_meta)
    with pytest.raises(ValueError):
        google_sheets.sort_sheet_by_column("sid", "Missing", 0)


# ---- Utility helpers ----


def test_get_sheet_id_by_name(monkeypatch):
    service = mock.MagicMock()
    service.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "MyTab", "sheetId": 123}}]
    }
    result = google_sheets.get_sheet_id_by_name(service, "sid", "MyTab")
    assert result == 123


def test_get_sheet_id_by_name_not_found(monkeypatch):
    service = mock.MagicMock()
    service.spreadsheets.return_value.get.return_value.execute.return_value = {"sheets": []}
    with pytest.raises(ValueError):
        google_sheets.get_sheet_id_by_name(service, "sid", "X")


def test_rename_sheet(mock_service):
    mock_service.spreadsheets.return_value.batchUpdate.return_value.execute.return_value = {
        "done": True
    }
    google_sheets.rename_sheet(mock_service, "sid", 42, "Renamed")
    mock_service.spreadsheets.return_value.batchUpdate.assert_called_once()


# ---- Insert / Get / Clear ----


def test_insert_rows_success(mock_service):
    mock_service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = (
        {}
    )
    google_sheets.insert_rows(mock_service, "sid", "Tab", [["1", "2"]])
    mock_service.spreadsheets.return_value.values.return_value.update.assert_called_once()


def test_insert_rows_http_error(mock_service):
    mock_service.spreadsheets.return_value.values.return_value.update.side_effect = HttpError(
        mock.Mock(), b"fail"
    )
    with pytest.raises(HttpError):
        google_sheets.insert_rows(mock_service, "sid", "Tab", [["1", "2"]])


def test_get_spreadsheet_metadata_success(mock_service):
    mock_service.spreadsheets.return_value.get.return_value.execute.return_value = {"sheets": []}
    assert google_sheets.get_spreadsheet_metadata(mock_service, "sid") == {"sheets": []}


def test_get_spreadsheet_metadata_http_error(mock_service):
    mock_service.spreadsheets.return_value.get.return_value.execute.side_effect = HttpError(
        mock.Mock(), b"fail"
    )
    with pytest.raises(HttpError):
        google_sheets.get_spreadsheet_metadata(mock_service, "sid")


def test_get_sheet_values(mock_service):
    mock_service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
        "values": [["A", None]]
    }
    vals = google_sheets.get_sheet_values(mock_service, "sid", "Tab")
    assert vals == [["A", ""]]


def test_clear_all_except_one_sheet(mock_service):
    fake_execute = {
        "sheets": [
            {"properties": {"title": "Keep", "sheetId": 1}},
            {"properties": {"title": "DeleteMe", "sheetId": 2}},
        ]
    }
    mock_service.spreadsheets.return_value.get.return_value.execute.return_value = fake_execute
    mock_service.spreadsheets.return_value.batchUpdate.return_value.execute.return_value = {}
    google_sheets.clear_all_except_one_sheet(mock_service, "sid", "Keep")


def test_clear_all_except_one_sheet_http_error(mock_service):
    mock_service.spreadsheets.return_value.get.side_effect = HttpError(mock.Mock(), b"fail")
    with pytest.raises(HttpError):
        google_sheets.clear_all_except_one_sheet(mock_service, "sid", "Keep")


def test_clear_sheet(mock_service):
    mock_service.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "Tab", "sheetId": 5}}]
    }
    google_sheets.clear_sheet(mock_service, "sid", "Tab")
    mock_service.spreadsheets.return_value.batchUpdate.assert_called_once()


def test_delete_sheet_by_name(mock_service):
    mock_service.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [
            {"properties": {"title": "A", "sheetId": 1}},
            {"properties": {"title": "B", "sheetId": 2}},
        ]
    }
    google_sheets.delete_sheet_by_name(mock_service, "sid", "A")
    mock_service.spreadsheets.return_value.batchUpdate.assert_called_once()


def test_delete_all_sheets_except(mock_service):
    mock_service.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [
            {"properties": {"title": "A", "sheetId": 1}},
            {"properties": {"title": "B", "sheetId": 2}},
        ]
    }
    google_sheets.delete_all_sheets_except(mock_service, "sid", "B")
    mock_service.spreadsheets.return_value.batchUpdate.assert_called_once()
