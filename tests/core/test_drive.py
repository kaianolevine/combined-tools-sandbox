# tests/test_core_drive.py

import pytest
from unittest import mock
from core import drive


@pytest.fixture
def mock_service():
    return mock.MagicMock()


def test_get_drive_service(monkeypatch, mock_service):
    monkeypatch.setattr("core.google_api.get_drive_service", lambda: mock_service)
    service = drive.get_drive_service()
    assert service == mock_service


def test_find_latest_m3u_file_no_files(mock_service, monkeypatch):
    monkeypatch.setattr("core.google_api.get_drive_service", lambda: mock_service)
    files_resource = mock_service.files.return_value
    files_resource.list.return_value.execute.return_value = {"files": []}
    folder_id = "folder123"

    result = drive.find_latest_m3u_file(folder_id)
    assert result is None


def test_find_latest_m3u_file_with_files(mock_service, monkeypatch):
    monkeypatch.setattr("core.google_api.get_drive_service", lambda: mock_service)
    file_list = {
        "files": [
            {"id": "1", "name": "2025-01-01.m3u"},
            {"id": "2", "name": "2025-01-02.m3u"},
        ]
    }
    files_resource = mock_service.files.return_value
    files_resource.list.return_value.execute.return_value = file_list
    folder_id = "folder123"

    result = drive.find_latest_m3u_file(folder_id)
    assert result == file_list["files"][0]


def test_download_file_success(monkeypatch, mock_service, tmp_path):
    fake_downloader = mock.MagicMock()
    progress1 = mock.Mock()
    progress1.progress.return_value = 0.5
    progress2 = mock.Mock()
    progress2.progress.return_value = 1.0
    fake_downloader.next_chunk.side_effect = [(progress1, False), (progress2, True)]
    monkeypatch.setattr("core.drive.MediaIoBaseDownload", lambda fh, req: fake_downloader)

    request_mock = mock.Mock()
    mock_service.files.return_value.get_media.return_value = request_mock

    file_id = "file123"
    dest = tmp_path / "out.txt"

    # 🔧 pass mock_service instead of ""
    drive.download_file(mock_service, file_id, str(dest))

    # Assertions
    mock_service.files.return_value.get_media.assert_called_once_with(fileId=file_id)
    assert dest.exists()


def test_list_files_in_folder_empty(mock_service, monkeypatch):
    monkeypatch.setattr("core.google_api.get_drive_service", lambda: mock_service)
    files_resource = mock_service.files.return_value
    files_resource.list.return_value.execute.return_value = {"files": []}
    result = drive.list_files_in_folder("folderid")
    assert result == []


def test_list_files_in_folder_with_files(mock_service, monkeypatch):
    monkeypatch.setattr("core.google_api.get_drive_service", lambda: mock_service)
    files_resource = mock_service.files.return_value
    files_resource.list.return_value.execute.return_value = {
        "files": [{"id": "1", "name": "f1"}, {"id": "2", "name": "f2"}]
    }
    result = drive.list_files_in_folder("folderid")
    assert len(result) == 2
    assert result[0]["id"] == "1"


def test_list_files_in_folder_with_mime_filter(mock_service, monkeypatch):
    monkeypatch.setattr("core.google_api.get_drive_service", lambda: mock_service)
    files_resource = mock_service.files.return_value
    files_resource.list.return_value.execute.return_value = {
        "files": [{"id": "1", "name": "doc1"}]
    }
    result = drive.list_files_in_folder("folderid", mime_type_filter="text/plain")
    assert result[0]["id"] == "1"
