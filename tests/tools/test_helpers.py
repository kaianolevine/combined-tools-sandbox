# tests/test_core_helpers.py

from unittest import mock
import tools.dj_set_processor.helpers as helpers


def test_get_shared_filled_fields_counts():
    data1 = ["a", "b", ""]
    data2 = ["a", "", "c"]
    indices = [{"index": 0}, {"index": 1}, {"index": 2}]
    assert helpers.get_shared_filled_fields(data1, data2, indices) == 1


def test_get_dedup_match_score_average():
    data1 = ["SongA", "Artist"]
    data2 = ["SongA", "Artist2"]
    indices = [{"index": 0}, {"index": 1}]
    score = helpers.get_dedup_match_score(data1, data2, indices)
    assert 0 <= score <= 1


def test_string_similarity_and_clean_title():
    assert helpers.string_similarity("abc", "abc") == 1
    assert helpers.clean_title(" Song ") == "song"


def test_hex_to_rgb_valid_and_invalid():
    rgb = helpers.hex_to_rgb("#fff3b0")
    assert isinstance(rgb, dict)
    assert all(0 <= v <= 1 for v in rgb.values())
    rgb3 = helpers.hex_to_rgb("#fff")
    assert isinstance(rgb3, dict)
    rgb_bad = helpers.hex_to_rgb("bad")
    assert abs(rgb_bad["red"] - 187/255) < 0.01
    assert abs(rgb_bad["green"] - 170/255) < 0.01
    assert abs(rgb_bad["blue"] - 221/255) < 0.01


def test__try_and_release_folder_lock():
    key = "TestFolder"
    assert helpers._try_lock_folder(key) is True
    assert helpers._try_lock_folder(key) is False  # second call fails until release
    helpers._release_folder_lock(key)
    assert helpers._try_lock_folder(key) is True


def test__clean_title_removes_parentheses():
    assert helpers._clean_title("Song (Remix)") == "Song"


def test_levenshtein_and__string_similarity():
    assert helpers.levenshtein_distance("kitten", "sitting") == 3
    assert helpers._string_similarity("abc", "abc") == 1
    assert helpers._string_similarity("", "abc") == 0


def test__get_shared_filled_fields_counts():
    row_a = ["Song", "Artist", ""]
    row_b = ["Song", "", "Year"]
    indices = [{"index": 0}, {"index": 1}, {"index": 2}]
    assert helpers._get_shared_filled_fields(row_a, row_b, indices) == 1


def test__get_dedup_match_score_similarity():
    row_a = ["Song (Remix)", "Artist"]
    row_b = ["Song", "Artist"]
    indices = [{"index": 0, "field": "Title"}, {"index": 1, "field": "Artist"}]
    score = helpers._get_dedup_match_score(row_a, row_b, indices)
    assert 0 <= score <= 1
    assert score > 0


def test_extract_date_and_title_with_and_without_date():
    date, title = helpers.extract_date_and_title("2025-01-01 My Set")
    assert date == "2025-01-01"
    assert "My Set" in title

    date2, title2 = helpers.extract_date_and_title("NoDateFile")
    assert date2 == ""
    assert title2 == "NoDateFile"


def test_try_and_release_folder_lock_with_drive(monkeypatch):
    mock_service = mock.Mock()
    mock_files = mock_service.files.return_value
    # First call: no lock files found
    mock_files.list.return_value.execute.return_value = {"files": []}
    monkeypatch.setattr(helpers.google_api, "get_drive_service", lambda: mock_service)
    monkeypatch.setattr(helpers.config, "DJ_SETS", "parent_id")
    monkeypatch.setattr(helpers, "get_or_create_subfolder", lambda svc, parent, name: "folder123")
    monkeypatch.setattr(helpers.config, "LOCK_FILE_NAME", "LOCK")

    assert helpers.try_lock_folder("FolderX")

    # Second call: lock file exists
    mock_files.list.return_value.execute.return_value = {"files": [{"id": "1", "name": "LOCK"}]}
    result = helpers.try_lock_folder("FolderX")
    assert result is False

    # Test release_folder_lock deletes
    mock_files.list.return_value.execute.return_value = {"files": [{"id": "1", "name": "LOCK"}]}
    helpers.release_folder_lock("FolderX")
    mock_service.files.return_value.delete.return_value.execute.assert_called()