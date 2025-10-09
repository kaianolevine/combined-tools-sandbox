# tests/test_core_sheets.py

import pytest
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


def test_update_row_success(monkeypatch):
    """It should call the Google Sheets API update method with correct params."""
    fake_execute = mock.Mock(return_value={"updatedRows": 1})

    class FakeUpdate:
        def execute(self):
            return fake_execute()

    class FakeValues:
        def update(self, **kwargs):
            return FakeUpdate()

    class FakeSpreadsheets:
        def values(self):
            return FakeValues()

    class FakeService:
        def spreadsheets(self):
            return FakeSpreadsheets()

    # Patch get_sheets_service() to return an object with spreadsheets()
    fake_service = FakeService()
    monkeypatch.setattr(sheets, "get_sheets_service", lambda: fake_service)

    result = sheets.update_row(
        "spreadsheet123",
        "Processed!A2:C2",
        [["file.m3u", "2025-10-09", "last_line"]],
    )

    # Check that the mocked execute was called
    fake_execute.assert_called_once()
    assert result == {"updatedRows": 1}


def test_update_row_passes_correct_arguments(monkeypatch):
    """It should pass the spreadsheetId, range, and values correctly."""
    captured_args = {}

    def fake_update(**kwargs):
        captured_args.update(kwargs)
        return mock.Mock(execute=lambda: {"ok": True})

    class FakeValues:
        def update(self, **kwargs):
            return fake_update(**kwargs)

    class FakeSpreadsheets:
        def values(self):
            return FakeValues()

    class FakeService:
        def spreadsheets(self):
            return FakeSpreadsheets()

    fake_service = FakeService()
    monkeypatch.setattr(sheets, "get_sheets_service", lambda: fake_service)

    values = [["file.m3u", "2025-10-09", "last_line"]]
    sheets.update_row("spreadsheet123", "Processed!A2:C2", values)

    assert captured_args["spreadsheetId"] == "spreadsheet123"
    assert captured_args["range"] == "Processed!A2:C2"
    assert captured_args["valueInputOption"] == "USER_ENTERED"
    assert captured_args["body"] == {"values": values}


def test_update_row_raises_http_error(monkeypatch):
    """It should propagate exceptions if the API call fails."""

    class FakeUpdate:
        def execute(self):
            raise sheets.HttpError(resp=mock.Mock(status=500), content=b"internal error")

    class FakeValues:
        def update(self, **kwargs):
            return FakeUpdate()

    class FakeSpreadsheets:
        def values(self):
            return FakeValues()

    class FakeService:
        def spreadsheets(self):
            return FakeSpreadsheets()

    fake_service = FakeService()
    monkeypatch.setattr(sheets, "get_sheets_service", lambda: fake_service)

    with pytest.raises(sheets.HttpError):
        sheets.update_row("spreadsheet123", "Processed!A2:C2", [["bad"]])


def test_sort_sheet_by_column_success(monkeypatch):
    """It should build a correct batchUpdate request and execute it."""
    # Setup to capture arguments
    captured_args = {}

    class FakeBatchUpdate:
        def __init__(self, **kwargs):
            captured_args.update(kwargs)

        def execute(self):
            return {"status": "ok"}

    class FakeSpreadsheets:
        def batchUpdate(self, **kwargs):
            return FakeBatchUpdate(**kwargs)

    class FakeService:
        def spreadsheets(self):
            return FakeSpreadsheets()

    fake_service = FakeService()

    # Patch helpers
    monkeypatch.setattr(sheets, "get_sheets_service", lambda: fake_service)
    monkeypatch.setattr(
        sheets,
        "get_sheet_metadata",
        lambda ssid: {
            "sheets": [
                {"properties": {"title": "Data", "sheetId": 42}},
                {"properties": {"title": "Other", "sheetId": 99}},
            ]
        },
    )

    result = sheets.sort_sheet_by_column("spreadsheet123", "Data", column_index=2, ascending=False)

    assert captured_args["spreadsheetId"] == "spreadsheet123"
    body = captured_args["body"]["requests"][0]["sortRange"]
    assert body["range"]["sheetId"] == 42
    assert body["sortSpecs"][0]["dimensionIndex"] == 2
    assert body["sortSpecs"][0]["sortOrder"] == "DESCENDING"
    assert result == {"status": "ok"}


def test_sort_sheet_by_column_with_end_row(monkeypatch):
    """It should include endRowIndex if provided."""
    captured_body = {}

    def fake_batch_update(**kwargs):
        captured_body.update(kwargs)
        return mock.Mock(execute=lambda: {"ok": True})

    class FakeSpreadsheets:
        def batchUpdate(self, **kwargs):
            return fake_batch_update(**kwargs)

    fake_service = mock.Mock(spreadsheets=lambda: FakeSpreadsheets())

    monkeypatch.setattr(sheets, "get_sheets_service", lambda: fake_service)
    monkeypatch.setattr(
        sheets,
        "get_sheet_metadata",
        lambda ssid: {"sheets": [{"properties": {"title": "Data", "sheetId": 123}}]},
    )

    sheets.sort_sheet_by_column(
        "spreadsheet123", "Data", column_index=0, ascending=True, end_row=100
    )

    sort_range = captured_body["body"]["requests"][0]["sortRange"]["range"]
    assert sort_range["endRowIndex"] == 100


def test_sort_sheet_by_column_sheet_not_found(monkeypatch):
    """It should raise ValueError if sheet not found."""
    monkeypatch.setattr(sheets, "get_sheets_service", lambda: mock.Mock())
    monkeypatch.setattr(sheets, "get_sheet_metadata", lambda ssid: {"sheets": []})

    with pytest.raises(ValueError):
        sheets.sort_sheet_by_column("spreadsheet123", "Missing", column_index=0)
