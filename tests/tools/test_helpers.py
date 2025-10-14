import pytest
from unittest.mock import Mock
from tools.dj_set_processor import helpers as h


# =====================================================
# Basic utilities
# =====================================================


def test_string_similarity_basic():
    assert h.string_similarity("abc", "abc") == 1
    assert h.string_similarity("abc", "xyz") < 0.5


def test_clean_title_lower_and_strip():
    assert h.clean_title("  Test ") == "test"


def test_hex_to_rgb_valid_6char():
    result = h.hex_to_rgb("#00FF00")
    assert pytest.approx(result["green"], 0.003) == 1.0


def test_hex_to_rgb_valid_3char():
    result = h.hex_to_rgb("#0f0")
    assert result["green"] == 1.0


def test_hex_to_rgb_invalid_returns_white():
    assert h.hex_to_rgb("invalid") == {"red": 1, "green": 1, "blue": 1}


def test_levenshtein_distance_and_similarity():
    assert h.levenshtein_distance("kitten", "sitting") == 3
    assert 0 <= h._string_similarity("abc", "abcd") <= 1


def test__clean_title_removes_parentheses():
    assert h._clean_title("Song (Remix)") == "Song"


def test_extract_date_and_title_with_date():
    date, title = h.extract_date_and_title("2025-10-13 My Song")
    assert date == "2025-10-13"
    assert "My Song" in title


def test_extract_date_and_title_without_date():
    date, title = h.extract_date_and_title("My Song.mp3")
    assert date == ""
    assert title == "My Song.mp3"


def test_extract_year_from_filename_logs(monkeypatch):
    calls = {}
    monkeypatch.setattr(h.log, "debug", lambda m: calls.setdefault("debug", True))
    result = h.extract_year_from_filename("2024-05-22_file.csv")
    assert result == "2024"
    assert calls["debug"]


# =====================================================
# Shared field scoring logic
# =====================================================


def test_get_shared_filled_fields_counts_correctly():
    data1 = ["a", "", "b"]
    data2 = ["a", "x", "b"]
    indices = [{"index": 0}, {"index": 1}, {"index": 2}]
    result = h.get_shared_filled_fields(data1, data2, indices)
    assert result == 2


def test_get_dedup_match_score_averages_similarity(monkeypatch):
    monkeypatch.setattr(h, "string_similarity", lambda a, b: 0.5)
    data1 = ["A", "B"]
    data2 = ["A", "C"]
    indices = [{"index": 0}, {"index": 1}]
    assert 0 <= h.get_dedup_match_score(data1, data2, indices) <= 1


def test__get_shared_filled_fields_partial_data():
    row_a = ["", "value"]
    row_b = ["text", ""]
    idx = [{"index": 0}, {"index": 1}]
    assert h._get_shared_filled_fields(row_a, row_b, idx) == 0


def test__get_dedup_match_score_exact_and_soft(monkeypatch):
    monkeypatch.setattr(h, "string_similarity", lambda a, b: 0.6)
    row_a = ["title", "artist"]
    row_b = ["title", "artist"]
    indices = [{"field": "Title", "index": 0}, {"field": "Artist", "index": 1}]
    score = h._get_dedup_match_score(row_a, row_b, indices)
    assert 0 <= score <= 1


def test__get_dedup_match_score_uses_clean_title(monkeypatch):
    monkeypatch.setattr(h, "string_similarity", lambda a, b: 0.1)
    monkeypatch.setattr(h, "clean_title", lambda t: t.replace("(remix)", "").strip())
    row_a = ["Song (Remix)", "artist"]
    row_b = ["Song", "artist"]
    idx = [{"field": "Title", "index": 0}, {"field": "Artist", "index": 1}]
    assert h._get_dedup_match_score(row_a, row_b, idx) > 0


# =====================================================
# Locking behavior
# =====================================================


def test__try_and_release_folder_lock():
    assert h._try_lock_folder("A")
    assert not h._try_lock_folder("A")  # second should fail
    h._release_folder_lock("A")
    assert h._try_lock_folder("A")  # can reacquire


def test_try_lock_folder_creates_and_skips(monkeypatch):
    fake_drive = Mock()
    fake_drive.files.return_value.list.return_value.execute.return_value = {"files": []}
    fake_drive.files.return_value.create.return_value.execute.return_value = {}
    monkeypatch.setattr(h.google_api, "get_drive_client", lambda: fake_drive)
    monkeypatch.setattr(h.drive, "get_or_create_subfolder", lambda s, p, n: "id")
    monkeypatch.setattr(h.config, "DJ_SETS_FOLDER_ID", "root")
    monkeypatch.setattr(h.config, "LOCK_FILE_NAME", "_lock")

    result = h.try_lock_folder("folder")
    assert result

    # Simulate locked case
    fake_drive.files.return_value.list.return_value.execute.return_value = {"files": [{"id": "1"}]}
    assert not h.try_lock_folder("folder")


def test_release_folder_lock_deletes(monkeypatch):
    fake_drive = Mock()
    fake_drive.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "123"}]
    }
    fake_drive.files.return_value.delete.return_value.execute.return_value = {}
    monkeypatch.setattr(h.google_api, "get_drive_client", lambda: fake_drive)
    monkeypatch.setattr(h.drive, "get_or_create_subfolder", lambda s, p, n: "id")
    monkeypatch.setattr(h.config, "DJ_SETS_FOLDER_ID", "root")
    h.release_folder_lock("folder")
    fake_drive.files.return_value.delete.assert_called()


def test_release_folder_lock_handles_HttpError(monkeypatch):
    fake_drive = Mock()
    fake_drive.files.return_value.list.return_value.execute.return_value = {"files": [{"id": "1"}]}
    fake_drive.files.return_value.delete.side_effect = h.HttpError(Mock(), b"fail")
    monkeypatch.setattr(h.google_api, "get_drive_client", lambda: fake_drive)
    monkeypatch.setattr(h.drive, "get_or_create_subfolder", lambda s, p, n: "id")
    monkeypatch.setattr(h.config, "DJ_SETS_FOLDER_ID", "root")
    h.release_folder_lock("folder")  # should log error but not raise


# =====================================================
# normalize_csv
# =====================================================


def test_normalize_csv_reads_and_writes(tmp_path, monkeypatch):
    p = tmp_path / "f.csv"
    p.write_text("A  B\n\nC   D\n")
    monkeypatch.setattr(h.log, "debug", lambda m: None)
    monkeypatch.setattr(h.log, "info", lambda m: None)
    h.normalize_csv(str(p))
    data = p.read_text()
    assert "A B" in data and "C D" in data


# =====================================================
# normalize_prefixes_in_source
# =====================================================


def test_normalize_prefixes_in_source_handles_prefixes(monkeypatch):
    drive = Mock()
    drive.files.return_value.list.return_value.execute.side_effect = [
        {"files": [{"id": "1", "name": "FAILED_test.csv"}]},  # first list call
        {"files": []},  # exists_resp
    ]
    drive.files.return_value.update.return_value.execute.return_value = {}

    monkeypatch.setattr(h.config, "CSV_SOURCE_FOLDER_ID", "source")
    monkeypatch.setattr(h.log, "info", lambda m: None)
    monkeypatch.setattr(h.log, "debug", lambda m: None)
    monkeypatch.setattr(h.log, "warning", lambda m: None)
    monkeypatch.setattr(h.log, "error", lambda m: None)

    h.normalize_prefixes_in_source(drive)
    drive.files.return_value.update.assert_called()


def test_normalize_prefixes_in_source_handles_existing_target(monkeypatch):
    drive = Mock()
    drive.files.return_value.list.return_value.execute.side_effect = [
        {"files": [{"id": "1", "name": "Copy of test.csv"}]},
        {"files": [{"id": "2", "name": "test.csv"}]},  # exists
    ]
    monkeypatch.setattr(h.config, "CSV_SOURCE_FOLDER_ID", "source")
    monkeypatch.setattr(h.log, "info", lambda m: None)
    monkeypatch.setattr(h.log, "debug", lambda m: None)
    h.normalize_prefixes_in_source(drive)
    drive.files.return_value.update.assert_not_called()


def test_normalize_prefixes_in_source_handles_error(monkeypatch):
    drive = Mock()
    drive.files.return_value.list.side_effect = Exception("boom")
    monkeypatch.setattr(h.config, "CSV_SOURCE_FOLDER_ID", "source")
    monkeypatch.setattr(h.log, "error", lambda m: None)
    h.normalize_prefixes_in_source(drive)  # should log error but not raise
