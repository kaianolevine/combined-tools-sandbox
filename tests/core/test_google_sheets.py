import pytest
from unittest import mock
from googleapiclient.errors import HttpError
from core import google_sheets


@pytest.fixture
def mock_service():
    service = mock.MagicMock()
    sheets = service.spreadsheets.return_value
    sheets.get.return_value.execute.return_value = {"sheets": [{"properties": {"title": "Sheet1", "sheetId": 123}}]}
    return service


@pytest.fixture
def patch_get_service(monkeypatch, mock_service):
    monkeypatch.setattr(google_sheets, "get_sheets_service", lambda: mock_service)
    return mock_service


def test_get_sheets_service(monkeypatch):
    mock_fn = mock.MagicMock()
    monkeypatch.setattr("core._google_credentials.get_sheets_service", mock_fn)
    result = google_sheets.get_sheets_service()
    assert mock_fn.called
    assert result == mock_fn()


def test_get_gspread_client(monkeypatch):
    creds = mock.MagicMock()
    gclient = mock.MagicMock()
    monkeypatch.setattr("core._google_credentials.load_credentials", lambda: creds)
    monkeypatch.setattr("gspread.authorize", lambda c: gclient)
    assert google_sheets.get_gspread_client() == gclient


def test_get_or_create_sheet_creates_new(patch_get_service):
    svc = patch_get_service
    svc.spreadsheets.return_value.get.return_value.execute.return_value = {"sheets": []}
    google_sheets.get_or_create_sheet("id", "Test")
    assert svc.spreadsheets.return_value.batchUpdate.called


def test_get_or_create_sheet_existing(patch_get_service):
    svc = patch_get_service
    svc.spreadsheets.return_value.get.return_value.execute.return_value = {"sheets": [{"properties": {"title": "Test"}}]}
    google_sheets.get_or_create_sheet("id", "Test")
    assert not svc.spreadsheets.return_value.batchUpdate.called


def test_read_sheet(patch_get_service):
    svc = patch_get_service
    svc.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {"values": [[1, 2]]}
    result = google_sheets.read_sheet("id", "A1:B2")
    assert result == [[1, 2]]


def test_write_sheet(patch_get_service):
    svc = patch_get_service
    svc.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {"updatedRows": 1}
    result = google_sheets.write_sheet("id", "A1:B1", [["A", "B"]])
    assert result == {"updatedRows": 1}


def test_append_rows(patch_get_service):
    svc = patch_get_service
    svc.spreadsheets.return_value.values.return_value.append.return_value.execute.return_value = {"updates": {}}
    google_sheets.append_rows("id", "A1", [["x", "y"]])
    assert svc.spreadsheets.return_value.values.return_value.append.called


def test_log_debug_and_info(monkeypatch):
    called = {}
    monkeypatch.setattr(google_sheets, "get_or_create_sheet", lambda sid, sname: called.setdefault("created", True))
    monkeypatch.setattr(google_sheets, "append_rows", lambda sid, rng, rows: called.setdefault("rows", rows))
    google_sheets.log_debug("id", "debug msg")
    google_sheets.log_info("id", "info msg")
    assert "rows" in called


def test_ensure_sheet_exists_adds_headers(monkeypatch):
    called = {}
    monkeypatch.setattr(google_sheets, "get_or_create_sheet", lambda *a, **kw: True)
    monkeypatch.setattr(google_sheets, "read_sheet", lambda *a, **kw: [])
    monkeypatch.setattr(google_sheets, "write_sheet", lambda sid, rng, vals: called.setdefault("wrote", vals))
    google_sheets.ensure_sheet_exists("id", "Tab", ["a", "b"])
    assert "wrote" in called


def test_ensure_sheet_exists_skips_existing(monkeypatch):
    monkeypatch.setattr(google_sheets, "get_or_create_sheet", lambda *a, **kw: True)
    monkeypatch.setattr(google_sheets, "read_sheet", lambda *a, **kw: [["a", "b"]])
    monkeypatch.setattr(google_sheets, "write_sheet", mock.MagicMock())
    google_sheets.ensure_sheet_exists("id", "Tab", ["a", "b"])
    google_sheets.write_sheet.assert_not_called()


def test_get_sheet_metadata(patch_get_service):
    svc = patch_get_service
    result = google_sheets.get_sheet_metadata("id")
    assert "sheets" in result


def test_update_row(patch_get_service):
    svc = patch_get_service
    svc.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {"ok": 1}
    result = google_sheets.update_row("id", "A2", [["x"]])
    assert result == {"ok": 1}


def test_sort_sheet_by_column(monkeypatch):
    mock_meta = {"sheets": [{"properties": {"title": "Data", "sheetId": 1}}]}
    monkeypatch.setattr(google_sheets, "get_sheets_service", lambda: mock.MagicMock())
    monkeypatch.setattr(google_sheets, "get_sheet_metadata", lambda _id: mock_meta)
    svc = google_sheets.get_sheets_service()
    svc.spreadsheets.return_value.batchUpdate.return_value.execute.return_value = {"done": True}
    result = google_sheets.sort_sheet_by_column("id", "Data", 0)
    assert result["done"]


def test_get_sheet_id_by_name_found():
    svc = mock.MagicMock()
    svc.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "T", "sheetId": 5}}]
    }
    assert google_sheets.get_sheet_id_by_name(svc, "id", "T") == 5


def test_get_sheet_id_by_name_not_found():
    svc = mock.MagicMock()
    svc.spreadsheets.return_value.get.return_value.execute.return_value = {"sheets": []}
    with pytest.raises(ValueError):
        google_sheets.get_sheet_id_by_name(svc, "id", "missing")


def test_rename_sheet_executes():
    svc = mock.MagicMock()
    google_sheets.rename_sheet(svc, "id", 1, "New")
    assert svc.spreadsheets.return_value.batchUpdate.called


def test_insert_rows_success():
    svc = mock.MagicMock()
    google_sheets.insert_rows(svc, "id", "Tab", [["a"]])
    assert svc.spreadsheets.return_value.values.return_value.update.called


def test_insert_rows_http_error(monkeypatch):
    svc = mock.MagicMock()
    svc.spreadsheets.return_value.values.return_value.update.side_effect = HttpError(
        mock.Mock(status=500), b"boom"
    )
    with pytest.raises(HttpError):
        google_sheets.insert_rows(svc, "id", "Tab", [["a"]])


def test_get_spreadsheet_metadata_success():
    svc = mock.MagicMock()
    svc.spreadsheets.return_value.get.return_value.execute.return_value = {"meta": True}
    assert google_sheets.get_spreadsheet_metadata(svc, "id") == {"meta": True}


def test_get_spreadsheet_metadata_error():
    svc = mock.MagicMock()
    svc.spreadsheets.return_value.get.return_value.execute.side_effect = HttpError(
        mock.Mock(status=500), b"error"
    )
    with pytest.raises(HttpError):
        google_sheets.get_spreadsheet_metadata(svc, "id")


def test_get_sheet_values_normalizes():
    svc = mock.MagicMock()
    svc.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
        "values": [[1, None]]
    }
    vals = google_sheets.get_sheet_values(svc, "id", "Tab")
    assert vals == [["1", ""]]


def test_clear_all_except_one_sheet(monkeypatch):
    svc = mock.MagicMock()
    svc.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [
            {"properties": {"title": "Keep", "sheetId": 1}},
            {"properties": {"title": "Del", "sheetId": 2}},
        ]
    }
    google_sheets.clear_all_except_one_sheet(svc, "id", "Keep")
    assert svc.spreadsheets.return_value.batchUpdate.called


def test_clear_all_except_one_sheet_http_error(monkeypatch):
    svc = mock.MagicMock()
    svc.spreadsheets.return_value.get.side_effect = HttpError(mock.Mock(status=404), b"fail")
    with pytest.raises(HttpError):
        google_sheets.clear_all_except_one_sheet(svc, "id", "Keep")


def test_clear_sheet(monkeypatch):
    svc = mock.MagicMock()
    svc.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "T", "sheetId": 1}}]
    }
    google_sheets.clear_sheet(svc, "id", "T")
    assert svc.spreadsheets.return_value.batchUpdate.called


def test_clear_sheet_missing():
    svc = mock.MagicMock()
    svc.spreadsheets.return_value.get.return_value.execute.return_value = {"sheets": []}
    with pytest.raises(ValueError):
        google_sheets.clear_sheet(svc, "id", "Nope")


def test_delete_sheet_by_name_success():
    svc = mock.MagicMock()
    svc.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [
            {"properties": {"title": "A", "sheetId": 1}},
            {"properties": {"title": "B", "sheetId": 2}},
        ]
    }
    google_sheets.delete_sheet_by_name(svc, "id", "A")
    assert svc.spreadsheets.return_value.batchUpdate.called


def test_delete_sheet_by_name_skip_single_sheet():
    svc = mock.MagicMock()
    svc.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "A", "sheetId": 1}}]
    }
    google_sheets.delete_sheet_by_name(svc, "id", "A")
    svc.spreadsheets.return_value.batchUpdate.assert_not_called()


def test_delete_sheet_by_name_http_error(monkeypatch):
    svc = mock.MagicMock()
    svc.spreadsheets.return_value.get.side_effect = HttpError(mock.Mock(status=500), b"fail")
    with pytest.raises(HttpError):
        google_sheets.delete_sheet_by_name(svc, "id", "A")


def test_delete_all_sheets_except():
    svc = mock.MagicMock()
    svc.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [
            {"properties": {"title": "Keep", "sheetId": 1}},
            {"properties": {"title": "Del", "sheetId": 2}},
        ]
    }
    google_sheets.delete_all_sheets_except(svc, "id", "Keep")
    assert svc.spreadsheets.return_value.batchUpdate.called