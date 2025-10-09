# tests/test_sync.py

import pytest
from unittest import mock
import tools.westie_radio.sync as sync


def test_initialize_spreadsheet_deletes_sheet(monkeypatch):
    fake_service = mock.Mock()
    fake_metadata = {
        "sheets": [
            {"properties": {"title": "Sheet1", "sheetId": 123}},
            {"properties": {"title": "Other", "sheetId": 456}},
        ]
    }
    monkeypatch.setattr(sync.sheets, "ensure_sheet_exists", lambda *a, **k: None)
    monkeypatch.setattr(sync.sheets, "get_sheet_metadata", lambda ssid: fake_metadata)
    monkeypatch.setattr(sync.sheets, "delete_sheet_by_id", mock.Mock())
    monkeypatch.setattr(sync.sheets, "log_debug", mock.Mock())

    sync.initialize_spreadsheet()
    sync.sheets.delete_sheet_by_id.assert_called_once_with(sync.spreadsheet_id, 123)


def test_initialize_spreadsheet_handles_http_error(monkeypatch):
    monkeypatch.setattr(sync.sheets, "ensure_sheet_exists", lambda *a, **k: None)
    monkeypatch.setattr(
        sync.sheets,
        "get_sheet_metadata",
        mock.Mock(side_effect=sync.HttpError(resp=mock.Mock(status=404), content=b"not found")),
    )
    monkeypatch.setattr(sync.sheets, "log_debug", mock.Mock())

    # Should not raise, just logs
    sync.initialize_spreadsheet()


def test_main_no_folder_id(monkeypatch):
    monkeypatch.setattr(sync, "initialize_spreadsheet", lambda: None)
    monkeypatch.setattr(sync.sheets, "log_info", mock.Mock())
    monkeypatch.setattr(sync.sheets, "log_debug", mock.Mock())
    monkeypatch.setattr(sync.config, "M3U_FOLDER_ID", "")

    with pytest.raises(ValueError):
        sync.main()


def test_main_no_files(monkeypatch):
    monkeypatch.setattr(sync, "initialize_spreadsheet", lambda: None)
    monkeypatch.setattr(sync.sheets, "log_info", mock.Mock())
    monkeypatch.setattr(sync.sheets, "log_debug", mock.Mock())
    monkeypatch.setattr(sync.config, "M3U_FOLDER_ID", "folderid")
    monkeypatch.setattr(sync.drive, "list_files_in_folder", lambda fid: [])
    monkeypatch.setattr(sync.sheets, "read_sheet", lambda ssid, rng: [])
    monkeypatch.setattr(sync.google_api, "extract_date_from_filename", lambda f: "2025-01-01")
    monkeypatch.setattr(sync.google_api, "parse_m3u", lambda sheets, fname, ssid: [])

    sync.main()
    sync.sheets.log_info.assert_any_call(sync.spreadsheet_id, "❌ No .m3u files found.")


def test_main_processes_file(monkeypatch):
    monkeypatch.setattr(sync, "initialize_spreadsheet", lambda: None)
    monkeypatch.setattr(sync.sheets, "log_info", mock.Mock())
    monkeypatch.setattr(sync.sheets, "log_debug", mock.Mock())
    monkeypatch.setattr(sync.config, "M3U_FOLDER_ID", "folderid")

    fake_file = {"id": "fid", "name": "2025-01-01.m3u"}
    monkeypatch.setattr(sync.drive, "list_files_in_folder", lambda fid: [fake_file])
    monkeypatch.setattr(sync.sheets, "read_sheet", lambda *a, **k: [])
    monkeypatch.setattr(sync.google_api, "extract_date_from_filename", lambda f: "2025-01-01")
    monkeypatch.setattr(
        sync.google_api, "parse_m3u", lambda *a, **k: [("Artist", "Title", "<extvdjline>")]
    )
    monkeypatch.setattr(sync.drive, "download_file", lambda *a, **k: None)

    # Spotify mocks
    monkeypatch.setattr(sync.spotify, "search_track", lambda a, t: "spotify:track:123")
    monkeypatch.setattr(sync.spotify, "add_tracks_to_playlist", lambda uris: None)
    monkeypatch.setattr(sync.spotify, "trim_playlist_to_limit", lambda **kw: None)

    monkeypatch.setattr(sync.sheets, "append_rows", lambda *a, **k: None)
    monkeypatch.setattr(sync.sheets, "update_row", lambda *a, **k: None)
    monkeypatch.setattr(sync.sheets, "sort_sheet_by_column", lambda *a, **k: None)

    sync.main()
    # At least one track found and logged
    sync.sheets.log_info.assert_any_call(sync.spreadsheet_id, "✅ Found 1 tracks, ❌ 0 unfound")


def test_main_handles_spotify_error(monkeypatch):
    monkeypatch.setattr(sync, "initialize_spreadsheet", lambda: None)
    monkeypatch.setattr(sync.sheets, "log_info", mock.Mock())
    monkeypatch.setattr(sync.sheets, "log_debug", mock.Mock())
    monkeypatch.setattr(sync.config, "M3U_FOLDER_ID", "folderid")

    fake_file = {"id": "fid", "name": "2025-01-01.m3u"}
    monkeypatch.setattr(sync.drive, "list_files_in_folder", lambda fid: [fake_file])
    monkeypatch.setattr(sync.sheets, "read_sheet", lambda *a, **k: [])
    monkeypatch.setattr(sync.google_api, "extract_date_from_filename", lambda f: "2025-01-01")
    monkeypatch.setattr(
        sync.google_api, "parse_m3u", lambda *a, **k: [("Artist", "Title", "<extvdjline>")]
    )
    monkeypatch.setattr(sync.drive, "download_file", lambda *a, **k: None)

    monkeypatch.setattr(sync.spotify, "search_track", lambda a, t: None)
    monkeypatch.setattr(
        sync.spotify, "add_tracks_to_playlist", mock.Mock(side_effect=Exception("fail"))
    )
    monkeypatch.setattr(sync.spotify, "trim_playlist_to_limit", lambda **kw: None)

    monkeypatch.setattr(sync.sheets, "append_rows", lambda *a, **k: None)
    monkeypatch.setattr(sync.sheets, "update_row", lambda *a, **k: None)
    monkeypatch.setattr(sync.sheets, "sort_sheet_by_column", lambda *a, **k: None)

    sync.main()
    sync.sheets.log_debug.assert_any_call(sync.spreadsheet_id, mock.ANY)
