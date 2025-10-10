import pytest
from unittest import mock
from googleapiclient.errors import HttpError
from core import sheets_formatting


@pytest.fixture
def mock_sheets_service():
    return mock.MagicMock()


@pytest.fixture
def mock_sheet():
    sheet = mock.MagicMock()
    sheet._properties = {"sheetId": 123}
    sheet.spreadsheet.batch_update = mock.MagicMock()
    return sheet


# --- apply_sheet_formatting ---------------------------------------------------


def test_apply_sheet_formatting_applies_correct_formatting(mock_sheet):
    sheets_formatting.apply_sheet_formatting(mock_sheet)
    mock_sheet.format.assert_any_call(
        "A:Z", {"textFormat": {"fontSize": 10}, "horizontalAlignment": "LEFT"}
    )
    mock_sheet.freeze.assert_called_with(rows=1)
    mock_sheet.format.assert_any_call("1:1", {"textFormat": {"bold": True}})
    mock_sheet.spreadsheet.batch_update.assert_called_once()


# --- apply_formatting_to_sheet ------------------------------------------------


def test_apply_formatting_to_sheet_success(monkeypatch):
    fake_gc = mock.MagicMock()
    fake_sheet = mock.MagicMock()
    fake_sh = mock.MagicMock()
    fake_gc.open_by_key.return_value = fake_sh
    fake_sh.sheet1 = fake_sheet
    fake_sheet.get_all_values.return_value = [["Header", "Data"]]
    monkeypatch.setattr("core.sheets_formatting.google_sheets.get_gspread_client", lambda: fake_gc)

    sheets_formatting.apply_formatting_to_sheet("spreadsheet123")

    fake_gc.open_by_key.assert_called_with("spreadsheet123")
    fake_sheet.format.assert_any_call("1:1", {"textFormat": {"bold": True}})


def test_apply_formatting_to_sheet_empty(monkeypatch):
    fake_gc = mock.MagicMock()
    fake_sheet = mock.MagicMock()
    fake_sh = mock.MagicMock()
    fake_gc.open_by_key.return_value = fake_sh
    fake_sh.sheet1 = fake_sheet
    fake_sheet.get_all_values.return_value = []
    monkeypatch.setattr("core.sheets_formatting.google_sheets.get_gspread_client", lambda: fake_gc)

    # Should not raise an error
    sheets_formatting.apply_formatting_to_sheet("spreadsheet123")


def test_apply_formatting_to_sheet_raises(monkeypatch, caplog):
    monkeypatch.setattr(
        "core.sheets_formatting.google_sheets.get_gspread_client",
        mock.Mock(side_effect=Exception("boom")),
    )
    sheets_formatting.apply_formatting_to_sheet("spreadsheet123")
    assert "Error applying formatting" in caplog.text


# --- set_values ---------------------------------------------------------------


def test_set_values_constructs_correct_range(mock_sheets_service):
    values = [["A", "B"], ["C", "D"]]
    sheets_formatting.set_values(mock_sheets_service, "sid", "Sheet1", 1, 1, values)
    mock_sheets_service.spreadsheets().values().update.assert_called_once()
    args, kwargs = mock_sheets_service.spreadsheets().values().update.call_args
    assert "Sheet1" in kwargs["range"]
    assert kwargs["body"]["values"] == values


# --- set_bold_font ------------------------------------------------------------


def test_set_bold_font_sends_correct_request(mock_sheets_service):
    sheets_formatting.set_bold_font(mock_sheets_service, "sid", 1, 1, 3, 1, 2)
    mock_sheets_service.spreadsheets().batchUpdate.assert_called_once()
    body = mock_sheets_service.spreadsheets().batchUpdate.call_args[1]["body"]
    assert "repeatCell" in str(body)


# --- freeze_rows --------------------------------------------------------------


def test_freeze_rows(mock_sheets_service):
    sheets_formatting.freeze_rows(mock_sheets_service, "sid", 1, 2)
    body = mock_sheets_service.spreadsheets().batchUpdate.call_args[1]["body"]
    assert (
        body["requests"][0]["updateSheetProperties"]["properties"]["gridProperties"][
            "frozenRowCount"
        ]
        == 2
    )


# --- set_horizontal_alignment -------------------------------------------------


def test_set_horizontal_alignment(mock_sheets_service):
    sheets_formatting.set_horizontal_alignment(mock_sheets_service, "sid", 1, 1, 5, 1, 3, "CENTER")
    mock_sheets_service.spreadsheets().batchUpdate.assert_called_once()


# --- set_number_format --------------------------------------------------------


def test_set_number_format(mock_sheets_service):
    sheets_formatting.set_number_format(mock_sheets_service, "sid", 1, 1, 3, 1, 3, "0.00")
    mock_sheets_service.spreadsheets().batchUpdate.assert_called_once()


# --- auto_resize_columns ------------------------------------------------------


def test_auto_resize_columns(mock_sheets_service):
    sheets_formatting.auto_resize_columns(mock_sheets_service, "sid", 1, 3)
    mock_sheets_service.spreadsheets().batchUpdate.assert_called_once()


# --- update_sheet_values ------------------------------------------------------


def test_update_sheet_values(mock_sheets_service):
    values = [["A", "B"]]
    sheets_formatting.update_sheet_values(mock_sheets_service, "sid", "Sheet1", values)
    mock_sheets_service.spreadsheets().values().update.assert_called_once()
    kwargs = mock_sheets_service.spreadsheets().values().update.call_args[1]
    assert kwargs["valueInputOption"] == "USER_ENTERED"


# --- set_sheet_formatting -----------------------------------------------------


def test_set_sheet_formatting(mock_sheets_service, monkeypatch):
    monkeypatch.setattr(
        "core.sheets_formatting.google_sheets.get_sheets_service", lambda: mock_sheets_service
    )
    monkeypatch.setattr(
        "tools.dj_set_processor.helpers.hex_to_rgb", lambda c: {"red": 1, "green": 0, "blue": 0}
    )

    sheets_formatting.set_sheet_formatting("sid", 1, 1, 5, 5, [["#FFFFFF"], ["#000000"]])
    mock_sheets_service.spreadsheets().batchUpdate.assert_called_once()
    body = mock_sheets_service.spreadsheets().batchUpdate.call_args[1]["body"]
    assert "autoResizeDimensions" in str(body)


# --- set_column_formatting ----------------------------------------------------


def test_set_column_formatting_success(mock_sheets_service):
    mock_sheets_service.spreadsheets().get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": "Sheet1", "sheetId": 1}}]
    }
    sheets_formatting.set_column_formatting(mock_sheets_service, "sid", "Sheet1", 3)
    mock_sheets_service.spreadsheets().batchUpdate.assert_called_once()


def test_set_column_formatting_sheet_not_found(mock_sheets_service):
    mock_sheets_service.spreadsheets().get.return_value.execute.return_value = {"sheets": []}
    sheets_formatting.set_column_formatting(mock_sheets_service, "sid", "Unknown", 2)
    mock_sheets_service.spreadsheets().batchUpdate.assert_not_called()


def test_set_column_formatting_http_error(mock_sheets_service):
    mock_sheets_service.spreadsheets().get.side_effect = HttpError(mock.Mock(), b"boom")
    with pytest.raises(HttpError):
        sheets_formatting.set_column_formatting(mock_sheets_service, "sid", "Sheet1", 2)


# --- reorder_sheets -----------------------------------------------------------


def test_reorder_sheets_success(mock_sheets_service):
    metadata = {
        "sheets": [
            {"properties": {"title": "A", "sheetId": 1}},
            {"properties": {"title": "B", "sheetId": 2}},
        ]
    }
    sheets_formatting.reorder_sheets(mock_sheets_service, "sid", ["B", "A"], metadata)
    mock_sheets_service.spreadsheets().batchUpdate.assert_called_once()


def test_reorder_sheets_http_error(mock_sheets_service):
    metadata = {"sheets": [{"properties": {"title": "A", "sheetId": 1}}]}
    mock_sheets_service.spreadsheets().batchUpdate.side_effect = HttpError(mock.Mock(), b"boom")
    with pytest.raises(HttpError):
        sheets_formatting.reorder_sheets(mock_sheets_service, "sid", ["A"], metadata)


# --- format_summary_sheet -----------------------------------------------------


def test_format_summary_sheet(monkeypatch, mock_sheets_service):
    monkeypatch.setattr(
        "core.sheets_formatting.google_sheets.get_sheet_id_by_name", lambda *args, **kwargs: 123
    )
    sheets_formatting.format_summary_sheet(
        mock_sheets_service, "sid", "Sheet1", ["Header1", "Header2"], [["A", "B"]]
    )
    mock_sheets_service.spreadsheets().batchUpdate.assert_called_once()
    body = mock_sheets_service.spreadsheets().batchUpdate.call_args[1]["body"]
    assert "autoResizeDimensions" in str(body)
