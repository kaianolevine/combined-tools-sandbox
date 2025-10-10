import pytest
from unittest import mock
from googleapiclient.errors import HttpError
from core import google_drive


@pytest.fixture
def mock_service():
    return mock.MagicMock()


# ---- BASIC DRIVE CLIENT ----


def test_get_drive_service(monkeypatch):
    fake_client = mock.MagicMock()
    monkeypatch.setattr("core._google_credentials.get_drive_client", lambda: fake_client)
    assert google_drive.get_drive_service() == fake_client


# ---- FILE LISTING ----


def test_list_files_in_folder_success(monkeypatch, mock_service):
    mock_service.files.return_value.list.return_value.execute.side_effect = [
        {"files": [{"id": "1", "name": "A"}], "nextPageToken": None}
    ]
    files = google_drive.list_files_in_folder(mock_service, "folder123")
    assert len(files) == 1
    assert files[0]["name"] == "A"


def test_list_files_in_folder_error(monkeypatch, mock_service):
    mock_service.files.return_value.list.return_value.execute.side_effect = Exception("Boom")
    files = google_drive.list_files_in_folder(mock_service, "folder123")
    assert files == []


def test_list_music_files(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "x"}]
    }
    res = google_drive.list_music_files(mock_service, "folder1")
    assert res == [{"id": "x"}]


# ---- FOLDER CREATION ----


def test_get_or_create_folder_found(monkeypatch, mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "abc"}]
    }
    fid = google_drive.get_or_create_folder("p123", "Test", mock_service)
    assert fid == "abc"


def test_get_or_create_folder_created(monkeypatch, mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {"files": []}
    mock_service.files.return_value.create.return_value.execute.return_value = {"id": "newid"}
    fid = google_drive.get_or_create_folder("p123", "New", mock_service)
    assert fid == "newid"


def test_get_or_create_subfolder_found(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "subid"}]
    }
    res = google_drive.get_or_create_subfolder(mock_service, "parent", "Sub")
    assert res == "subid"


def test_get_or_create_subfolder_created(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {"files": []}
    mock_service.files.return_value.create.return_value.execute.return_value = {"id": "created"}
    res = google_drive.get_or_create_subfolder(mock_service, "parent", "Sub")
    assert res == "created"


# ---- FILE RETRIEVAL ----


def test_get_file_by_name_found(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "f1"}]
    }
    res = google_drive.get_file_by_name(mock_service, "parent", "file.txt")
    assert res["id"] == "f1"


def test_get_file_by_name_not_found(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {"files": []}
    assert google_drive.get_file_by_name(mock_service, "parent", "missing.txt") is None


def test_get_all_subfolders_success(mock_service):
    mock_service.files.return_value.list.return_value.execute.side_effect = [
        {"files": [{"id": "1"}], "nextPageToken": None}
    ]
    res = google_drive.get_all_subfolders(mock_service, "parent")
    assert res == [{"id": "1"}]


def test_get_all_subfolders_http_error(mock_service):
    mock_service.files.return_value.list.return_value.execute.side_effect = HttpError(
        mock.Mock(), b"fail"
    )
    with pytest.raises(HttpError):
        google_drive.get_all_subfolders(mock_service, "parent")


def test_get_files_in_folder_filters(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "1"}]
    }
    res = google_drive.get_files_in_folder(
        mock_service, "folder", name_contains="test", mime_type="text/csv"
    )
    assert res[0]["id"] == "1"


# ---- FILE DOWNLOAD ----


def test_download_file_success(monkeypatch, mock_service, tmp_path):
    fake_downloader = mock.MagicMock()
    fake_status = mock.Mock()
    fake_status.progress.side_effect = [0.5, 1.0]
    fake_downloader.next_chunk.side_effect = [(fake_status, False), (fake_status, True)]
    monkeypatch.setattr("core.google_drive.MediaIoBaseDownload", lambda fh, req: fake_downloader)
    fh = tmp_path / "out.txt"
    google_drive.download_file(mock_service, "file123", str(fh))
    fake_downloader.next_chunk.assert_called()


def test_download_file_io_error(monkeypatch, mock_service, tmp_path):
    monkeypatch.setattr("io.FileIO", lambda *a, **kw: (_ for _ in ()).throw(OSError("bad")))
    with pytest.raises(IOError):
        google_drive.download_file(mock_service, "f", str(tmp_path / "bad.txt"))


# ---- FILE UPLOAD ----


def test_upload_file(mock_service, tmp_path):
    fp = tmp_path / "data.csv"
    fp.write_text("a,b")
    monkeypatch = mock.patch(
        "core.google_drive.MediaFileUpload", lambda *a, **kw: mock.MagicMock()
    )
    with monkeypatch:
        google_drive.upload_file(mock_service, str(fp), "folder")


def test_upload_to_drive(monkeypatch, mock_service):
    monkeypatch.setattr("core.google_drive.MediaFileUpload", lambda *a, **kw: mock.MagicMock())
    monkeypatch.setattr("core.google_sheets.get_gspread_client", lambda: mock.MagicMock())
    mock_service.files.return_value.create.return_value.execute.return_value = {"id": "123"}
    sid = google_drive.upload_to_drive(mock_service, "some.csv", "parent")
    assert sid == "123"


# ---- FILE / SPREADSHEET CREATION ----


def test_create_spreadsheet_found(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "f1"}]
    }
    fid = google_drive.create_spreadsheet(mock_service, "test", "parent")
    assert fid == "f1"


def test_create_spreadsheet_created(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {"files": []}
    mock_service.files.return_value.create.return_value.execute.return_value = {"id": "newf"}
    fid = google_drive.create_spreadsheet(mock_service, "t", "p")
    assert fid == "newf"


def test_create_spreadsheet_http_error(mock_service):
    mock_service.files.return_value.list.return_value.execute.side_effect = HttpError(
        mock.Mock(), b"fail"
    )
    with pytest.raises(HttpError):
        google_drive.create_spreadsheet(mock_service, "x", "p")


def test_find_or_create_file_by_name_found(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "found"}]
    }
    fid = google_drive.find_or_create_file_by_name(mock_service, "a", "b")
    assert fid == "found"


def test_find_or_create_file_by_name_created(mock_service):
    mock_service.files.return_value.list.return_value.execute.return_value = {"files": []}
    mock_service.files.return_value.create.return_value.execute.return_value = {"id": "created"}
    fid = google_drive.find_or_create_file_by_name(mock_service, "a", "b")
    assert fid == "created"


def test_find_or_create_file_by_name_http_error(mock_service):
    mock_service.files.return_value.list.return_value.execute.side_effect = HttpError(
        mock.Mock(), b"fail"
    )
    with pytest.raises(HttpError):
        google_drive.find_or_create_file_by_name(mock_service, "x", "y")


# ---- FILE MOVEMENT ----


def test_move_file_to_folder(mock_service):
    mock_service.files.return_value.get.return_value.execute.return_value = {"parents": ["old"]}
    google_drive.move_file_to_folder(mock_service, "f1", "new")
    mock_service.files.return_value.update.assert_called_once()


def test_remove_file_from_root(mock_service):
    mock_service.files.return_value.get.return_value.execute.return_value = {"parents": ["root"]}
    google_drive.remove_file_from_root(mock_service, "fileid")
    mock_service.files.return_value.update.assert_called_once()


def test_remove_file_from_root_no_root(mock_service):
    mock_service.files.return_value.get.return_value.execute.return_value = {"parents": ["abc"]}
    google_drive.remove_file_from_root(mock_service, "fileid")
    mock_service.files.return_value.update.assert_not_called()


# ---- MISC HELPERS ----


def test_extract_date_from_filename_valid():
    assert google_drive.extract_date_from_filename("2025-01-01_event.csv") == "2025-01-01"


def test_extract_date_from_filename_invalid():
    assert google_drive.extract_date_from_filename("no-date.csv") == "no-date.csv"
