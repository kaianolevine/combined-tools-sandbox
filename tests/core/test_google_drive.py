import pytest
from unittest.mock import Mock
from googleapiclient.errors import HttpError
from core import google_drive as gd


# Ensure FOLDER_CACHE is cleared before each test to avoid cross-test pollution
@pytest.fixture(autouse=True)
def clear_folder_cache():
    gd.FOLDER_CACHE.clear()


# =====================================================
# get_drive_service
# =====================================================


def test_get_drive_service(monkeypatch):
    monkeypatch.setattr("core.google_drive.google_api.get_drive_client", lambda: "service")
    assert gd.get_drive_service() == "service"


# =====================================================
# extract_date_from_filename
# =====================================================


def test_extract_date_from_filename_with_date():
    assert gd.extract_date_from_filename("2025-10-13 File.csv") == "2025-10-13"


def test_extract_date_from_filename_without_date():
    assert gd.extract_date_from_filename("NoDateFile.csv") == "NoDateFile.csv"


# =====================================================
# list_files_in_folder
# =====================================================


def test_list_files_in_folder_basic(monkeypatch):
    service = Mock()
    service.files.return_value.list.return_value.execute.side_effect = [
        {"files": [{"id": "1", "name": "test.csv"}], "nextPageToken": None}
    ]
    files = gd.list_files_in_folder(service, "folder123")
    assert files[0]["id"] == "1"


def test_list_files_in_folder_with_filter(monkeypatch):
    service = Mock()
    service.files.return_value.list.return_value.execute.return_value = {
        "files": [],
        "nextPageToken": None,
    }
    gd.list_files_in_folder(service, "folder", mime_type_filter="text/csv")
    service.files.return_value.list.assert_called()


def test_list_files_in_folder_handles_exception(monkeypatch):
    service = Mock()
    service.files.return_value.list.side_effect = Exception("boom")
    result = gd.list_files_in_folder(service, "folder")
    assert result == []


# =====================================================
# list_music_files
# =====================================================


def test_list_music_files_returns_files():
    service = Mock()
    service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "1", "name": "song.mp3"}]
    }
    files = gd.list_music_files(service, "folder")
    assert files[0]["name"].endswith(".mp3")


# =====================================================
# get_or_create_folder
# =====================================================


def test_get_or_create_folder_uses_cache(monkeypatch):
    gd.FOLDER_CACHE.clear()
    gd.FOLDER_CACHE["p/name"] = "cached_id"
    result = gd.get_or_create_folder("p", "name", Mock())
    assert result == "cached_id"


def test_get_or_create_folder_finds_existing(monkeypatch):
    service = Mock()
    service.files.return_value.list.return_value.execute.return_value = {"files": [{"id": "123"}]}
    result = gd.get_or_create_folder("p", "name", service)
    assert result == "123"


def test_get_or_create_folder_creates_new(monkeypatch):
    service = Mock()
    service.files.return_value.list.return_value.execute.return_value = {"files": []}
    service.files.return_value.create.return_value.execute.return_value = {"id": "new_id"}
    result = gd.get_or_create_folder("p", "name", service)
    assert result == "new_id"


# =====================================================
# get_or_create_subfolder
# =====================================================


def test_get_or_create_subfolder_existing():
    service = Mock()
    service.files.return_value.list.return_value.execute.return_value = {"files": [{"id": "1"}]}
    assert gd.get_or_create_subfolder(service, "parent", "sub") == "1"


def test_get_or_create_subfolder_creates_new():
    service = Mock()
    service.files.return_value.list.return_value.execute.return_value = {"files": []}
    service.files.return_value.create.return_value.execute.return_value = {"id": "x"}
    assert gd.get_or_create_subfolder(service, "parent", "newsub") == "x"


# =====================================================
# get_file_by_name
# =====================================================


def test_get_file_by_name_found():
    s = Mock()
    s.files.return_value.list.return_value.execute.return_value = {"files": [{"id": "1"}]}
    assert gd.get_file_by_name(s, "f", "name")["id"] == "1"


def test_get_file_by_name_not_found():
    s = Mock()
    s.files.return_value.list.return_value.execute.return_value = {"files": []}
    assert gd.get_file_by_name(s, "f", "name") is None


# =====================================================
# get_all_subfolders
# =====================================================


def test_get_all_subfolders_multiple_pages(monkeypatch):
    s = Mock()
    s.files.return_value.list.return_value.execute.side_effect = [
        {"files": [{"id": "1"}], "nextPageToken": "next"},
        {"files": [{"id": "2"}], "nextPageToken": None},
    ]
    result = gd.get_all_subfolders(s, "parent")
    assert len(result) == 2


def test_get_all_subfolders_handles_http_error(monkeypatch):
    s = Mock()
    s.files.return_value.list.side_effect = HttpError(Mock(), b"fail")
    with pytest.raises(HttpError):
        gd.get_all_subfolders(s, "parent")


# =====================================================
# get_files_in_folder
# =====================================================


def test_get_files_in_folder_filters(monkeypatch):
    s = Mock()
    s.files.return_value.list.return_value.execute.return_value = {"files": [{"id": "1"}]}
    files = gd.get_files_in_folder(s, "parent", name_contains="test", mime_type="text/csv")
    assert files[0]["id"] == "1"


# =====================================================
# download_file
# =====================================================


def test_download_file_success(monkeypatch, tmp_path):
    s = Mock()
    downloader_mock = Mock()
    downloader_mock.next_chunk.side_effect = [
        (Mock(progress=lambda: 0.5), False),
        (Mock(progress=lambda: 1.0), True),
    ]
    monkeypatch.setattr(gd, "MediaIoBaseDownload", lambda fh, req: downloader_mock)
    s.files.return_value.get_media.return_value = "req"
    dest = tmp_path / "out.bin"
    gd.download_file(s, "fileid", str(dest))
    assert dest.exists()


def test_download_file_raises_io_error(monkeypatch):
    s = Mock()
    monkeypatch.setattr(gd, "MediaIoBaseDownload", Mock())
    s.files.return_value.get_media.return_value = "req"
    with pytest.raises(IOError):
        gd.download_file(s, "fileid", "/bad/path/does/not/exist/file.txt")


# =====================================================
# upload_file
# =====================================================


def test_upload_file_calls_create(monkeypatch):
    s = Mock()
    monkeypatch.setattr(gd, "MediaFileUpload", lambda f, resumable=True: f)
    gd.upload_file(s, "test.csv", "folder")
    s.files.return_value.create.assert_called()


# =====================================================
# upload_to_drive
# =====================================================


def test_upload_to_drive_removes_sep(monkeypatch):
    s = Mock()
    s.files.return_value.create.return_value.execute.return_value = {"id": "1"}
    monkeypatch.setattr(gd, "MediaFileUpload", lambda f, mimetype=None: f)
    gc = Mock()
    sheet = Mock()
    sheet.row_values.return_value = ["sep=,"]
    gc.open_by_key.return_value.worksheets.return_value = [sheet]
    monkeypatch.setattr(gd.google_sheets, "get_gspread_client", lambda: gc)
    result = gd.upload_to_drive(s, "file.csv", "parent")
    assert result == "1"


# =====================================================
# create_spreadsheet
# =====================================================


def test_create_spreadsheet_existing(monkeypatch):
    s = Mock()
    s.files.return_value.list.return_value.execute.return_value = {"files": [{"id": "x"}]}
    assert gd.create_spreadsheet(s, "Name", "Parent") == "x"


def test_create_spreadsheet_creates(monkeypatch):
    s = Mock()
    s.files.return_value.list.return_value.execute.return_value = {"files": []}
    s.files.return_value.create.return_value.execute.return_value = {"id": "new"}
    assert gd.create_spreadsheet(s, "Name", "Parent") == "new"


def test_create_spreadsheet_http_error(monkeypatch):
    s = Mock()
    s.files.return_value.list.side_effect = HttpError(Mock(), b"fail")
    with pytest.raises(HttpError):
        gd.create_spreadsheet(s, "N", "P")


# =====================================================
# move_file_to_folder / remove_file_from_root
# =====================================================


def test_move_file_to_folder(monkeypatch):
    s = Mock()
    s.files.return_value.get.return_value.execute.return_value = {"parents": ["a"]}
    gd.move_file_to_folder(s, "file", "folder")
    s.files.return_value.update.assert_called()


def test_remove_file_from_root(monkeypatch):
    s = Mock()
    s.files.return_value.get.return_value.execute.return_value = {"parents": ["root", "other"]}
    gd.remove_file_from_root(s, "file")
    s.files.return_value.update.assert_called()


def test_remove_file_from_root_no_root(monkeypatch):
    s = Mock()
    s.files.return_value.get.return_value.execute.return_value = {"parents": ["other"]}
    gd.remove_file_from_root(s, "file")
    s.files.return_value.update.assert_not_called()


# =====================================================
# find_or_create_file_by_name
# =====================================================


def test_find_or_create_file_by_name_existing(monkeypatch):
    s = Mock()
    s.files.return_value.list.return_value.execute.return_value = {"files": [{"id": "1"}]}
    assert gd.find_or_create_file_by_name(s, "File", "Parent") == "1"


def test_find_or_create_file_by_name_creates(monkeypatch):
    s = Mock()
    s.files.return_value.list.return_value.execute.return_value = {"files": []}
    s.files.return_value.create.return_value.execute.return_value = {"id": "new"}
    assert gd.find_or_create_file_by_name(s, "File", "Parent") == "new"


def test_find_or_create_file_by_name_http_error(monkeypatch):
    s = Mock()
    s.files.return_value.list.side_effect = HttpError(Mock(), b"fail")
    with pytest.raises(HttpError):
        gd.find_or_create_file_by_name(s, "File", "Parent")


# =====================================================
# find_subfolder_id
# =====================================================


def test_find_subfolder_id_found(monkeypatch):
    s = Mock()
    s.files.return_value.list.return_value.execute.return_value = {"files": [{"id": "x"}]}
    assert gd.find_subfolder_id(s, "parent", "sub") == "x"


def test_find_subfolder_id_not_found(monkeypatch):
    s = Mock()
    s.files.return_value.list.return_value.execute.return_value = {"files": []}
    assert gd.find_subfolder_id(s, "parent", "sub") is None


def test_find_subfolder_id_error(monkeypatch):
    s = Mock()
    s.files.return_value.list.side_effect = Exception("boom")
    assert gd.find_subfolder_id(s, "parent", "sub") is None
