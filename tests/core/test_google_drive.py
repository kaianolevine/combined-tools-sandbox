import io
import os
import pytest
from unittest import mock
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import core.google_drive as drive


@pytest.fixture
def mock_service():
    """Create a mock Google Drive service with chained calls."""
    service = mock.MagicMock()
    service.files.return_value = mock.MagicMock()
    return service


def make_http_error(*args, **kwargs):
    raise HttpError(mock.Mock(status=404), b"File not found")


# --- get_drive_service ---
def test_get_drive_service_loads_credentials(monkeypatch):
    mock_build = mock.MagicMock()
    monkeypatch.setattr(drive.google_api, "load_credentials", lambda: "creds")
    monkeypatch.setattr(drive, "build", mock_build)
    drive.get_drive_service()
    mock_build.assert_called_once_with("drive", "v3", credentials="creds")


# --- extract_date_from_filename ---
def test_extract_date_from_filename_valid():
    assert drive.extract_date_from_filename("2024-10-12_Song.csv") == "2024-10-12"


def test_extract_date_from_filename_invalid():
    assert drive.extract_date_from_filename("no_date_file.csv") == "no_date_file.csv"


# --- list_files_in_folder ---
def test_list_files_in_folder_success(mock_service):
    fake_files = [{"id": "1", "name": "file1"}]
    mock_service.files.return_value.list.return_value.execute.side_effect = [
        {"files": fake_files, "nextPageToken": None}
    ]
    results = drive.list_files_in_folder(mock_service, "folder123")
    assert results == fake_files
    mock_service.files.return_value.list.assert_called()


def test_list_files_in_folder_error(mock_service):
    mock_service.files.return_value.list.side_effect = Exception("API failure")
    results = drive.list_files_in_folder(mock_service, "folder123")
    assert results == []


# --- list_music_files ---
def test_list_music_files_returns_files(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "1", "name": "track.mp3"}]
    }
    results = drive.list_music_files(mock_service, "folder123")
    assert results[0]["name"] == "track.mp3"


# --- get_or_create_folder ---
def test_get_or_create_folder_finds_existing(monkeypatch, mock_service):
    drive.FOLDER_CACHE.clear()
    mock_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "existing_id", "name": "Folder"}]
    }
    folder_id = drive.get_or_create_folder("parent123", "Folder", mock_service)
    assert folder_id == "existing_id"


def test_get_or_create_folder_creates_new(monkeypatch, mock_service):
    drive.FOLDER_CACHE.clear()
    mock_service.files.return_value.list.return_value.execute.return_value = {"files": []}
    mock_service.files.return_value.create.return_value.execute.return_value = {"id": "new_id"}
    folder_id = drive.get_or_create_folder("parent123", "NewFolder", mock_service)
    assert folder_id == "new_id"


# --- get_or_create_subfolder ---
def test_get_or_create_subfolder_finds_existing(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "sub_id", "name": "Sub"}]
    }
    assert drive.get_or_create_subfolder(mock_service, "parent", "Sub") == "sub_id"


def test_get_or_create_subfolder_creates_new(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {"files": []}
    mock_service.files.return_value.create.return_value.execute.return_value = {"id": "new_sub"}
    assert drive.get_or_create_subfolder(mock_service, "parent", "NewSub") == "new_sub"


# --- get_file_by_name ---
def test_get_file_by_name_found(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "1", "name": "match"}]
    }
    assert drive.get_file_by_name(mock_service, "folder", "match")["id"] == "1"


def test_get_file_by_name_not_found(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {"files": []}
    assert drive.get_file_by_name(mock_service, "folder", "missing") is None


# --- get_all_subfolders ---
def test_get_all_subfolders_success(mock_service):
    mock_service.files.return_value.list.return_value.execute.side_effect = [
        {"files": [{"id": "a"}], "nextPageToken": None}
    ]
    result = drive.get_all_subfolders(mock_service, "parent")
    assert result[0]["id"] == "a"


def test_get_all_subfolders_http_error(mock_service):
    mock_service.files.return_value.list.side_effect = make_http_error
    with pytest.raises(HttpError):
        drive.get_all_subfolders(mock_service, "parent")


# --- get_files_in_folder ---
def test_get_files_in_folder_success(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "f1"}]
    }
    result = drive.get_files_in_folder(mock_service, "folder")
    assert result == [{"id": "f1"}]


# --- download_file ---
def test_download_file_success(monkeypatch, mock_service, tmp_path):
    path = tmp_path / "file.txt"
    request_mock = mock.Mock()
    downloader_mock = mock.Mock()
    downloader_mock.next_chunk.side_effect = [
        (mock.Mock(progress=lambda: 1.0), True)
    ]
    monkeypatch.setattr(drive, "MediaIoBaseDownload", lambda fh, req: downloader_mock)
    mock_service.files.return_value.get_media.return_value = request_mock

    drive.download_file(mock_service, "file123", str(path))
    downloader_mock.next_chunk.assert_called()


def test_download_file_io_error(monkeypatch, mock_service, tmp_path):
    monkeypatch.setattr(io, "FileIO", mock.Mock(side_effect=OSError("disk error")))
    with pytest.raises(IOError):
        drive.download_file(mock_service, "file123", str(tmp_path / "bad.txt"))


# --- upload_file ---
def test_upload_file_success(mock_service, tmp_path):
    file_path = tmp_path / "sample.txt"
    file_path.write_text("data")
    with mock.patch("core.google_drive.MediaFileUpload") as mfu:
        drive.upload_file(mock_service, str(file_path), "folder")
        mfu.assert_called_once()


# --- upload_to_drive ---
def test_upload_to_drive_removes_sep_rows(monkeypatch, mock_service, tmp_path):
    import importlib
    google_sheets = importlib.import_module("core.google_sheets")
    file_path = tmp_path / "upload.csv"
    file_path.write_text("sep=,\nA,B\n1,2")

    spreadsheet_mock = mock.Mock()
    sheet_mock = mock.Mock()
    sheet_mock.row_values.return_value = ["sep=,"]
    spreadsheet_mock.worksheets.return_value = [sheet_mock]

    gspread_mock = mock.Mock()
    gspread_mock.open_by_key.return_value = spreadsheet_mock
    monkeypatch.setattr(google_sheets, "get_gspread_client", lambda: gspread_mock)
    mock_service.files.return_value.create.return_value.execute.return_value = {"id": "123"}

    result = drive.upload_to_drive(mock_service, str(file_path), "folder")
    assert result == "123"
    sheet_mock.delete_rows.assert_called_once_with(1)


# --- create_spreadsheet ---
def test_create_spreadsheet_finds_existing(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "123"}]
    }
    assert drive.create_spreadsheet(mock_service, "Name", "Parent") == "123"


def test_create_spreadsheet_creates_new(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {"files": []}
    mock_service.files.return_value.create.return_value.execute.return_value = {"id": "new"}
    assert drive.create_spreadsheet(mock_service, "Name", "Parent") == "new"


def test_create_spreadsheet_http_error(mock_service):
    mock_service.files.return_value.list.side_effect = make_http_error
    with pytest.raises(HttpError):
        drive.create_spreadsheet(mock_service, "Name", "Parent")


# --- move_file_to_folder ---
def test_move_file_to_folder_moves(mock_service):
    mock_service.files.return_value.get.return_value.execute.return_value = {"parents": ["old"]}
    drive.move_file_to_folder(mock_service, "file", "new")
    mock_service.files.return_value.update.assert_called()


# --- remove_file_from_root ---
def test_remove_file_from_root_removes(mock_service):
    mock_service.files.return_value.get.return_value.execute.return_value = {"parents": ["root"]}
    drive.remove_file_from_root(mock_service, "file")
    mock_service.files.return_value.update.assert_called()


def test_remove_file_from_root_no_root(mock_service):
    mock_service.files.return_value.get.return_value.execute.return_value = {"parents": ["x"]}
    drive.remove_file_from_root(mock_service, "file")
    mock_service.files.return_value.update.assert_not_called()


# --- find_or_create_file_by_name ---
def test_find_or_create_file_by_name_existing(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "found"}]
    }
    assert drive.find_or_create_file_by_name(mock_service, "file", "parent") == "found"


def test_find_or_create_file_by_name_creates(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {"files": []}
    mock_service.files.return_value.create.return_value.execute.return_value = {"id": "new"}
    assert drive.find_or_create_file_by_name(mock_service, "file", "parent") == "new"


def test_find_or_create_file_by_name_http_error(mock_service):
    mock_service.files.return_value.list.side_effect = make_http_error
    with pytest.raises(HttpError):
        drive.find_or_create_file_by_name(mock_service, "file", "parent")