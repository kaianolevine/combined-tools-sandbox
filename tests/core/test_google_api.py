# tests/test_core_google_api.py

import json
import pytest
from unittest import mock

from core import google_api
from googleapiclient.errors import HttpError


# --- Credentials ---

def test_load_credentials_env(monkeypatch):
    creds_mock = mock.Mock()
    monkeypatch.setenv("GOOGLE_CREDENTIALS_JSON", json.dumps({"foo": "bar"}))
    monkeypatch.setattr(
        google_api.service_account.Credentials,
        "from_service_account_info",
        lambda *a, **k: creds_mock,
    )
    assert google_api.load_credentials() == creds_mock


def test_load_credentials_env_invalid_json(monkeypatch):
    monkeypatch.setenv("GOOGLE_CREDENTIALS_JSON", "not-json")
    monkeypatch.setattr(
        google_api.service_account.Credentials,
        "from_service_account_file",
        lambda *a, **k: "filecreds",
    )
    creds = google_api.load_credentials()
    assert creds == "filecreds"


def test_load_credentials_file(monkeypatch):
    creds_mock = mock.Mock()
    monkeypatch.delenv("GOOGLE_CREDENTIALS_JSON", raising=False)
    monkeypatch.setattr(
        google_api.service_account.Credentials,
        "from_service_account_file",
        lambda *a, **k: creds_mock,
    )
    assert google_api.load_credentials() == creds_mock


def test_load_credentials_file_missing(monkeypatch):
    monkeypatch.delenv("GOOGLE_CREDENTIALS_JSON", raising=False)
    monkeypatch.setattr(
        google_api.service_account.Credentials,
        "from_service_account_file",
        lambda *a, **k: (_ for _ in ()).throw(FileNotFoundError("missing")),
    )
    with pytest.raises(FileNotFoundError):
        google_api.load_credentials()


# --- Drive Helpers ---

def test_get_drive_and_sheets_service(monkeypatch):
    creds = mock.Mock()
    monkeypatch.setattr(google_api, "load_credentials", lambda: creds)
    build_mock = mock.Mock()
    monkeypatch.setattr(google_api, "build", build_mock)

    google_api.get_drive_service()
    build_mock.assert_called_with("drive", "v3", credentials=creds)

    google_api.get_sheets_service()
    build_mock.assert_called_with("sheets", "v4", credentials=creds)


def test_get_gspread_client(monkeypatch):
    creds = mock.Mock()
    monkeypatch.setattr(google_api, "load_credentials", lambda: creds)
    auth_mock = mock.Mock()
    monkeypatch.setattr(google_api.gspread, "authorize", lambda c: auth_mock)
    assert google_api.get_gspread_client() == auth_mock


def test_get_or_create_folder_existing(monkeypatch):
    drive_service = mock.Mock()
    drive_service.files().list().execute.return_value = {"files": [{"id": "123"}]}
    result = google_api.get_or_create_folder("parent", "child", drive_service)
    assert result == "123"


def test_get_or_create_folder_new(monkeypatch):
    drive_service = mock.Mock()
    drive_service.files().list().execute.return_value = {"files": []}
    drive_service.files().create().execute.return_value = {"id": "newid"}
    result = google_api.get_or_create_folder("parent", "child2", drive_service)
    assert result == "newid"


def make_http_error():
    resp = mock.Mock(status=404, reason="Not Found")
    content = b"Requested entity was not found."
    return HttpError(resp, content, uri="http://test")


def test_get_or_create_folder_create_failure(monkeypatch):
    drive = mock.Mock()
    drive.files().list().execute.return_value = {"files": []}
    drive.files().create().execute.side_effect = make_http_error()
    with pytest.raises(HttpError):
        google_api.get_or_create_folder("pid", "child", drive)


def test_upload_to_drive(monkeypatch, tmp_path):
    drive = mock.Mock()
    drive.files().create().execute.return_value = {"id": "fileid"}
    gc = mock.Mock()
    sheet_mock = mock.Mock()
    sheet_mock.row_values.return_value = ["sep=,"]
    gc.open_by_key().worksheets.return_value = [sheet_mock]

    monkeypatch.setattr(google_api, "get_gspread_client", lambda: gc)

    file = tmp_path / "file.csv"
    file.write_text("col1,col2\nval1,val2")
    file_id = google_api.upload_to_drive(drive, str(file), "parent")
    assert file_id == "fileid"


def test_list_files_in_drive_folder(monkeypatch):
    drive = mock.Mock()
    drive.files().list().execute.side_effect = [
        {"files": [{"id": "1"}], "nextPageToken": "tok"},
        {"files": [{"id": "2"}]},
    ]
    files = google_api.list_files_in_drive_folder(drive, "folder")
    assert {"id": "1"} in files and {"id": "2"} in files


def test_download_file(monkeypatch, tmp_path):
    drive = mock.Mock()
    request = mock.Mock()
    drive.files().get_media.return_value = request

    chunks = [(mock.Mock(progress=0.5), False), (mock.Mock(progress=1.0), True)]

    class FakeDownloader:
        def __init__(self, fh, req):
            self._chunks = iter(chunks)

        def next_chunk(self):
            return next(self._chunks)

    monkeypatch.setattr(google_api, "MediaIoBaseDownload", FakeDownloader)

    dest = tmp_path / "out.txt"
    google_api.download_file(drive, "fid", str(dest))
    assert dest.exists()


@pytest.mark.parametrize(
    "existing_files,expected_id",
    [
        ([{"id": "123"}], "123"),
        ([], "newid"),
    ],
)
def test_create_spreadsheet(monkeypatch, existing_files, expected_id):
    drive = mock.Mock()
    drive.files().list().execute.return_value = {"files": existing_files}
    if not existing_files:
        drive.files().create().execute.return_value = {"id": "newid"}
    assert google_api.create_spreadsheet(drive, "name", "folder") == expected_id


@pytest.mark.parametrize(
    "files_list,search_name,expected_result",
    [
        ([{"id": "1", "name": "foo"}], "foo", "1"),
        ([], "bar", None),
    ],
)
def test_get_file_by_name(monkeypatch, files_list, search_name, expected_result):
    drive = mock.Mock()
    drive.files().list().execute.return_value = {"files": files_list}
    result = google_api.get_file_by_name(drive, "folder", search_name)
    if expected_result is None:
        assert result is None
    else:
        assert result["id"] == expected_result


@pytest.mark.parametrize(
    "files_list,search_name,raises",
    [
        ([{"id": "1", "name": "foo"}], "foo", False),
        ([], "bar", True),
        ([{"id": "1", "name": "foo"}, {"id": "2", "name": "foo"}], "foo", False),
    ],
)
def test_find_file_by_name(monkeypatch, files_list, search_name, raises):
    drive_service = mock.Mock()
    drive_service.list_files_in_folder.return_value = files_list
    if raises:
        with pytest.raises(FileNotFoundError):
            google_api.find_file_by_name(drive_service, "folder", search_name)
    else:
        file = google_api.find_file_by_name(drive_service, "folder", search_name)
        assert file["name"] == search_name
        assert file["id"] in [f["id"] for f in files_list]


# --- Sheets Helpers ---

def test_delete_all_sheets_except(monkeypatch):
    sheets = mock.Mock()
    sheets.spreadsheets().get().execute.return_value = {
        "sheets": [
            {"properties": {"title": "keep", "sheetId": 1}},
            {"properties": {"title": "del", "sheetId": 2}},
        ]
    }
    google_api.delete_all_sheets_except(sheets, "ssid", "keep")
    assert sheets.spreadsheets().batchUpdate.called


def test_delete_all_sheets_except_missing(monkeypatch):
    sheets = mock.Mock()
    sheets.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "other", "sheetId": 5}}]
    }
    # No "keep" title, so all are deleted
    google_api.delete_all_sheets_except(sheets, "ssid", "keep")
    assert sheets.spreadsheets().batchUpdate.called


def test_set_values(monkeypatch):
    sheets = mock.Mock()
    google_api.set_values(sheets, "ssid", "sheet", 1, 1, [["a", "b"]])
    assert sheets.spreadsheets().values().update.called


def test_set_values_empty(monkeypatch):
    sheets = mock.Mock()
    google_api.set_values(sheets, "ssid", "sheet", 1, 1, [])
    # update should still be called even with empty list
    sheets.spreadsheets().values().update.assert_called_once()
    args, kwargs = sheets.spreadsheets().values().update.call_args
    # Check that empty values list was passed
    assert kwargs["body"]["values"] == []


def test_set_values_http_error(monkeypatch):
    sheets = mock.Mock()
    sheets.spreadsheets().values().update().execute.side_effect = make_http_error()
    with pytest.raises(HttpError):
        google_api.set_values(sheets, "ssid", "sheet", 1, 1, [["a"]])


def test_delete_sheet_by_name(monkeypatch):
    sheets = mock.Mock()
    # Case only one sheet
    sheets.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "only"}}]
    }
    google_api.delete_sheet_by_name(sheets, "ssid", "only")
    # Case with multiple sheets
    sheets.spreadsheets().get().execute.return_value = {
        "sheets": [
            {"properties": {"title": "delme", "sheetId": 2}},
            {"properties": {"title": "keep", "sheetId": 3}},
        ]
    }
    google_api.delete_sheet_by_name(sheets, "ssid", "delme")
    assert sheets.spreadsheets().batchUpdate.called


def test_delete_sheet_by_name_not_found(monkeypatch):
    sheets = mock.Mock()
    sheets.spreadsheets().get().execute.return_value = {
        "sheets": [{"properties": {"title": "x", "sheetId": 1}}]
    }
    # Title doesn't match → should not call batchUpdate
    google_api.delete_sheet_by_name(sheets, "ssid", "y")
    assert not sheets.spreadsheets().batchUpdate.called


# --- Formatting ---

def test_apply_formatting_to_sheet_empty(monkeypatch):
    gc = mock.Mock()
    sh = mock.Mock()
    sheet = mock.Mock()
    sheet.get_all_values.return_value = []
    sh.sheet1 = sheet
    gc.open_by_key.return_value = sh
    monkeypatch.setattr(google_api, "get_gspread_client", lambda: gc)
    google_api.apply_formatting_to_sheet("spreadsheetid")


def test_apply_formatting_to_sheet_with_data(monkeypatch):
    gc = mock.Mock()
    sh = mock.Mock()
    sheet = mock.Mock()
    sheet.get_all_values.return_value = [["a", "b"], ["c", "d"]]
    sh.sheet1 = sheet
    gc.open_by_key.return_value = sh
    monkeypatch.setattr(google_api, "get_gspread_client", lambda: gc)
    google_api.apply_formatting_to_sheet("ssid")
    assert sheet.format.called


# --- Parsing ---

def test_extract_date_from_filename():
    assert google_api.extract_date_from_filename("2025-01-01-file.csv") == "2025-01-01"
    assert google_api.extract_date_from_filename("nofile.csv") == "nofile.csv"


def test_parse_m3u(tmp_path):
    m3u = tmp_path / "test.m3u"
    content = """#EXTVDJ:<artist>Artist</artist><title>Title</title>\n"""
    m3u.write_text(content)
    sheets_service = mock.Mock()
    songs = google_api.parse_m3u(sheets_service, str(m3u), "ssid")
    assert songs[0][0] == "Artist"
    assert songs[0][1] == "Title"


def test_parse_m3u_invalid_file(tmp_path):
    m3u = tmp_path / "bad.m3u"
    m3u.write_text("#EXTVDJ:thisisnotxml\n")
    songs = google_api.parse_m3u(mock.Mock(), str(m3u), "ssid")
    # Expect empty result for invalid lines
    assert songs == []


def test_parse_m3u_empty_file(tmp_path):
    m3u = tmp_path / "empty.m3u"
    m3u.write_text("")
    songs = google_api.parse_m3u(mock.Mock(), str(m3u), "ssid")
    assert songs == []


# --- Download ---

def test_download_file_never_completes(monkeypatch, tmp_path):
    drive = mock.Mock()
    request = mock.Mock()
    drive.files().get_media.return_value = request

    # Simulate a downloader that returns incomplete progress twice then completes
    call_count = {"count": 0}

    class FakeDownloader:
        def __init__(self, fh, req):
            pass

        def next_chunk(self):
            call_count["count"] += 1
            if call_count["count"] < 3:
                return (mock.Mock(progress=0.5), False)  # incomplete
            else:
                return (mock.Mock(progress=1.0), True)  # complete

    monkeypatch.setattr(google_api, "MediaIoBaseDownload", FakeDownloader)

    dest = tmp_path / "out.txt"
    google_api.download_file(drive, "fid", str(dest))
    assert dest.exists()
    # Ensure next_chunk was called at least 3 times (retries)
    assert call_count["count"] >= 3
