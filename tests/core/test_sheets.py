# tests/test_core_sheets.py

from unittest import mock
import core.sheets as sheets


def test_get_sheets_service(monkeypatch):
    service = mock.Mock()
    monkeypatch.setattr(sheets.google_api, "get_sheets_service", lambda: service)
    result = sheets.get_sheets_service()
    assert result == service


def test_get_or_create_sheet_creates(monkeypatch):
    service = mock.Mock()
    service.spreadsheets().get().execute.return_value = {"sheets": []}
    monkeypatch.setattr(sheets, "get_sheets_service", lambda: service)

    sheets.get_or_create_sheet("ssid", "NewSheet")
    assert service.spreadsheets().batchUpdate.called


def test_get_or_create_sheet_exists(monkeypatch):
    service = mock.Mock()
    service.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "Existing"}}]
    }
    monkeypatch.setattr(sheets, "get_sheets_service", lambda: service)

    sheets.get_or_create_sheet("ssid", "Existing")
    assert not service.spreadsheets().batchUpdate.called


def test_read_sheet(monkeypatch):
    service = mock.Mock()
    service.spreadsheets().values().get().execute.return_value = {"values": [["a"]]}
    monkeypatch.setattr(sheets, "get_sheets_service", lambda: service)

    values = sheets.read_sheet("ssid", "range")
    assert values == [["a"]]


def test_write_sheet(monkeypatch):
    service = mock.Mock()
    service.spreadsheets().values().update().execute.return_value = {"updatedRows": 1}
    monkeypatch.setattr(sheets, "get_sheets_service", lambda: service)

    result = sheets.write_sheet("ssid", "range", [["a"]])
    assert result["updatedRows"] == 1


def test_append_rows(monkeypatch):
    service = mock.Mock()
    service.spreadsheets().values().append().execute.return_value = {"updates": {}}
    monkeypatch.setattr(sheets, "get_sheets_service", lambda: service)

    sheets.append_rows("ssid", "range", [["a"]])
    assert service.spreadsheets().values().append.called


def test_log_info(monkeypatch):
    monkeypatch.setattr(sheets, "get_or_create_sheet", lambda *a, **k: None)
    monkeypatch.setattr(sheets, "append_rows", lambda *a, **k: None)

    sheets.log_info("ssid", "msg")  # just check no exception


def test_log_processed(monkeypatch):
    monkeypatch.setattr(sheets, "get_or_create_sheet", lambda *a, **k: None)
    monkeypatch.setattr(sheets, "append_rows", lambda *a, **k: None)

    sheets.log_processed("ssid", "file.csv", "10:00")


def test_log_processed_full(monkeypatch):
    monkeypatch.setattr(sheets, "get_or_create_sheet", lambda *a, **k: None)
    monkeypatch.setattr(sheets, "append_rows", lambda *a, **k: None)

    sheets.log_processed_full("ssid", "file.csv", "ts", "last", "title", "artist")


def test_get_latest_processed_empty(monkeypatch):
    monkeypatch.setattr(sheets, "get_or_create_sheet", lambda *a, **k: None)
    monkeypatch.setattr(sheets, "read_sheet", lambda *a, **k: [])
    result = sheets.get_latest_processed("ssid")
    assert result is None


def test_get_latest_processed_with_rows(monkeypatch):
    monkeypatch.setattr(sheets, "get_or_create_sheet", lambda *a, **k: None)
    monkeypatch.setattr(sheets, "read_sheet", lambda *a, **k: [["file", "time"]])
    result = sheets.get_latest_processed("ssid")
    assert result == ["file", "time"]


def test_ensure_sheet_exists_writes_headers(monkeypatch):
    monkeypatch.setattr(sheets, "get_or_create_sheet", lambda *a, **k: None)
    monkeypatch.setattr(sheets, "read_sheet", lambda *a, **k: [])
    monkeypatch.setattr(sheets, "write_sheet", lambda *a, **k: [["hdr"]])
    sheets.ensure_sheet_exists("ssid", "tab", ["hdr"])


def test_ensure_sheet_exists_already_has_headers(monkeypatch):
    monkeypatch.setattr(sheets, "get_or_create_sheet", lambda *a, **k: None)
    monkeypatch.setattr(sheets, "read_sheet", lambda *a, **k: [["hdr"]])
    monkeypatch.setattr(sheets, "write_sheet", lambda *a, **k: [["hdr"]])
    sheets.ensure_sheet_exists("ssid", "tab", ["hdr"])  # no write expected


def test_ensure_log_sheet_exists_writes_headers(monkeypatch):
    monkeypatch.setattr(sheets, "get_or_create_sheet", lambda *a, **k: None)
    monkeypatch.setattr(sheets, "read_sheet", lambda *a, **k: [])
    monkeypatch.setattr(sheets, "write_sheet", lambda *a, **k: [["hdrs"]])
    sheets.ensure_log_sheet_exists("ssid")


def test_get_sheet_metadata(monkeypatch):
    service = mock.Mock()
    service.spreadsheets().get().execute.return_value = {"spreadsheetId": "ssid"}
    monkeypatch.setattr(sheets, "get_sheets_service", lambda: service)

    result = sheets.get_sheet_metadata("ssid")
    assert result["spreadsheetId"] == "ssid"


def test_delete_sheet_by_id(monkeypatch):
    service = mock.Mock()
    monkeypatch.setattr(sheets, "get_sheets_service", lambda: service)
    sheets.delete_sheet_by_id("ssid", 123)
    assert service.spreadsheets().batchUpdate.called