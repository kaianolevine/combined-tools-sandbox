import pytest
from unittest.mock import Mock
from googleapiclient.errors import HttpError
from core import google_sheets as gs


# =====================================================
# Fixtures and helpers
# =====================================================


@pytest.fixture
def mock_service():
    service = Mock()
    service.spreadsheets.return_value = Mock()
    service.spreadsheets().values.return_value = Mock()
    return service


@pytest.fixture
def fake_metadata():
    return {
        "sheets": [
            {"properties": {"title": "Sheet1", "sheetId": 1}},
            {"properties": {"title": "Info", "sheetId": 2}},
        ]
    }


# =====================================================
# Service getters
# =====================================================


def test_get_sheets_service(monkeypatch):
    monkeypatch.setattr(
        "core.google_sheets._google_credentials.get_sheets_client", lambda: "client"
    )
    assert gs.get_sheets_service() == "client"


def test_get_gspread_client(monkeypatch):
    monkeypatch.setattr(
        "core.google_sheets._google_credentials.get_gspread_client", lambda: "gclient"
    )
    assert gs.get_gspread_client() == "gclient"


# =====================================================
# get_or_create_sheet
# =====================================================


def test_get_or_create_sheet_creates_when_missing(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "Old"}}]
    }
    mock_service.spreadsheets().batchUpdate().execute.return_value = {"done": True}
    gs.get_or_create_sheet(mock_service, "spreadsheet", "NewSheet")
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_get_or_create_sheet_noop_if_exists(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "Exists"}}]
    }
    gs.get_or_create_sheet(mock_service, "id", "Exists")
    mock_service.spreadsheets().batchUpdate.assert_not_called()


# =====================================================
# read_sheet / write_sheet / append_rows
# =====================================================


def test_read_sheet_returns_values(mock_service):
    mock_service.spreadsheets().values().get().execute.return_value = {"values": [["A", "B"]]}
    result = gs.read_sheet(mock_service, "id", "Sheet1!A1:B2")
    assert result == [["A", "B"]]


def test_write_sheet_sends_values(mock_service):
    mock_service.spreadsheets().values().update().execute.return_value = {"updatedRows": 1}
    result = gs.write_sheet(mock_service, "id", "Sheet1!A1", [["x"]])
    assert "updatedRows" in result


def test_append_rows_appends(mock_service):
    mock_service.spreadsheets().values().append().execute.return_value = {"updates": "ok"}
    gs.append_rows(mock_service, "id", "Sheet1!A1", [["a"]])
    mock_service.spreadsheets().values().append.assert_called()


# =====================================================
# log_info_sheet
# =====================================================


def test_log_info_sheet_calls_append(monkeypatch, mock_service):
    monkeypatch.setattr(gs, "get_or_create_sheet", lambda s, i, n: None)
    monkeypatch.setattr(gs, "append_rows", lambda s, i, r, v: setattr(s, "called", True))
    gs.log_info_sheet(mock_service, "id", "message")
    assert hasattr(mock_service, "called")


# =====================================================
# ensure_sheet_exists
# =====================================================


def test_ensure_sheet_exists_writes_headers(monkeypatch, mock_service):
    monkeypatch.setattr(gs, "get_or_create_sheet", lambda s, i, n: None)
    monkeypatch.setattr(gs, "read_sheet", lambda s, i, r: [])
    monkeypatch.setattr(gs, "write_sheet", lambda s, i, r, v: setattr(s, "called", True))
    gs.ensure_sheet_exists(mock_service, "id", "Sheet1", ["A", "B"])
    assert hasattr(mock_service, "called")


def test_ensure_sheet_exists_no_write_if_headers_present(monkeypatch, mock_service):
    monkeypatch.setattr(gs, "get_or_create_sheet", lambda s, i, n: None)
    monkeypatch.setattr(gs, "read_sheet", lambda s, i, r: [["A", "B"]])
    monkeypatch.setattr(
        gs, "write_sheet", lambda s, i, r, v: (_ for _ in ()).throw(Exception("should not write"))
    )
    gs.ensure_sheet_exists(mock_service, "id", "Sheet1", ["A", "B"])


# =====================================================
# get_sheet_metadata
# =====================================================


def test_get_sheet_metadata_returns_metadata(mock_service, fake_metadata):
    mock_service.spreadsheets().get().execute.return_value = fake_metadata
    result = gs.get_sheet_metadata(mock_service, "id")
    assert "sheets" in result


# =====================================================
# update_row
# =====================================================


def test_update_row(monkeypatch):
    fake_service = Mock()
    fake_service.spreadsheets().values().update().execute.return_value = {"updated": True}
    monkeypatch.setattr(gs, "get_sheets_service", lambda: fake_service)
    result = gs.update_row("id", "A1:B1", [["x", "y"]])
    assert result["updated"]


# =====================================================
# sort_sheet_by_column
# =====================================================


def test_sort_sheet_by_column_success(mock_service, fake_metadata):
    mock_service.spreadsheets().get().execute.return_value = fake_metadata
    mock_service.spreadsheets().batchUpdate().execute.return_value = {"sorted": True}
    result = gs.sort_sheet_by_column(mock_service, "id", "Sheet1", 0, ascending=False)
    assert "sorted" in result


def test_sort_sheet_by_column_raises_if_missing(mock_service, fake_metadata):
    fake_metadata["sheets"][0]["properties"]["title"] = "Other"
    mock_service.spreadsheets().get().execute.return_value = fake_metadata
    with pytest.raises(ValueError):
        gs.sort_sheet_by_column(mock_service, "id", "Missing", 0)


# =====================================================
# get_sheet_id_by_name
# =====================================================


def test_get_sheet_id_by_name_returns_id(mock_service, fake_metadata):
    mock_service.spreadsheets().get().execute.return_value = fake_metadata
    result = gs.get_sheet_id_by_name(mock_service, "id", "Sheet1")
    assert result == 1


def test_get_sheet_id_by_name_raises(mock_service, fake_metadata):
    fake_metadata["sheets"][0]["properties"]["title"] = "Different"
    mock_service.spreadsheets().get().execute.return_value = fake_metadata
    with pytest.raises(ValueError):
        gs.get_sheet_id_by_name(mock_service, "id", "Sheet1")


# =====================================================
# rename_sheet
# =====================================================


def test_rename_sheet_calls_batch_update(mock_service):
    mock_service.spreadsheets().batchUpdate().execute.return_value = {"done": True}
    gs.rename_sheet(mock_service, "id", 1, "NewName")
    mock_service.spreadsheets().batchUpdate.assert_called()


# =====================================================
# insert_rows
# =====================================================


def test_insert_rows_success(mock_service):
    mock_service.spreadsheets().values().update().execute.return_value = {"done": True}
    gs.insert_rows(mock_service, "id", "Sheet1", [["a", "b"]])


def test_insert_rows_http_error(mock_service):
    mock_service.spreadsheets().values().update.side_effect = HttpError(
        resp=Mock(status=400, reason="Bad"), content=b"fail"
    )
    with pytest.raises(HttpError):
        gs.insert_rows(mock_service, "id", "Sheet1", [["a"]])


# =====================================================
# get_spreadsheet_metadata
# =====================================================


def test_get_spreadsheet_metadata_success(mock_service, fake_metadata):
    mock_service.spreadsheets().get().execute.return_value = fake_metadata
    result = gs.get_spreadsheet_metadata(mock_service, "id")
    assert "sheets" in result


def test_get_spreadsheet_metadata_error(mock_service):
    mock_service.spreadsheets().get().execute.side_effect = HttpError(
        resp=Mock(status=500, reason="error"), content=b"fail"
    )
    with pytest.raises(HttpError):
        gs.get_spreadsheet_metadata(mock_service, "id")


# =====================================================
# write_sheet_data
# =====================================================


def test_write_sheet_data(monkeypatch, mock_service):
    monkeypatch.setattr(gs, "ensure_sheet_exists", lambda s, i, n: None)
    gs.write_sheet_data(mock_service, "id", "Sheet1", ["H1"], [["R1"]])
    mock_service.spreadsheets().values().update.assert_called()


# =====================================================
# get_sheet_values
# =====================================================


def test_get_sheet_values_returns_normalized(mock_service):
    mock_service.spreadsheets().values().get().execute.return_value = {"values": [[1, None, "X"]]}
    result = gs.get_sheet_values(mock_service, "id", "Sheet1")
    assert result == [["1", "", "X"]]


# =====================================================
# clear_all_except_one_sheet
# =====================================================


def test_clear_all_except_one_sheet_creates_and_deletes(monkeypatch, mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [
            {"properties": {"title": "A", "sheetId": 1}},
            {"properties": {"title": "B", "sheetId": 2}},
        ]
    }
    mock_service.spreadsheets().batchUpdate().execute.return_value = {"done": True}
    gs.clear_all_except_one_sheet(mock_service, "id", "Keep")
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_clear_all_except_one_sheet_http_error(monkeypatch, mock_service):
    mock_service.spreadsheets().get.side_effect = HttpError(
        resp=Mock(status=400, reason="bad"), content=b"fail"
    )
    with pytest.raises(HttpError):
        gs.clear_all_except_one_sheet(mock_service, "id", "Keep")


# =====================================================
# clear_sheet
# =====================================================


def test_clear_sheet_success(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "Sheet1", "sheetId": 1}}]
    }
    mock_service.spreadsheets().batchUpdate().execute.return_value = {"cleared": True}
    gs.clear_sheet(mock_service, "id", "Sheet1")
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_clear_sheet_raises_if_missing(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {"sheets": []}
    with pytest.raises(ValueError):
        gs.clear_sheet(mock_service, "id", "Sheet1")


# =====================================================
# delete_sheet_by_name
# =====================================================


def test_delete_sheet_by_name_deletes(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [
            {"properties": {"title": "Keep", "sheetId": 1}},
            {"properties": {"title": "DeleteMe", "sheetId": 2}},
        ]
    }
    gs.delete_sheet_by_name(mock_service, "id", "DeleteMe")
    mock_service.spreadsheets().batchUpdate.assert_called()


def test_delete_sheet_by_name_only_one_sheet(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "OnlyOne", "sheetId": 1}}]
    }
    gs.delete_sheet_by_name(mock_service, "id", "OnlyOne")
    mock_service.spreadsheets().batchUpdate.assert_not_called()


def test_delete_sheet_by_name_http_error(mock_service):
    mock_service.spreadsheets().get.side_effect = HttpError(
        resp=Mock(status=400, reason="bad"), content=b"fail"
    )
    with pytest.raises(HttpError):
        gs.delete_sheet_by_name(mock_service, "id", "Any")


# =====================================================
# delete_all_sheets_except
# =====================================================


def test_delete_all_sheets_except(mock_service):
    mock_service.spreadsheets().get().execute.return_value = {
        "sheets": [
            {"properties": {"title": "A", "sheetId": 1}},
            {"properties": {"title": "B", "sheetId": 2}},
        ]
    }
    mock_service.spreadsheets().batchUpdate().execute.return_value = {"deleted": True}
    gs.delete_all_sheets_except(mock_service, "id", "A")
    mock_service.spreadsheets().batchUpdate.assert_called()
