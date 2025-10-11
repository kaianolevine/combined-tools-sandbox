import pytest
from unittest import mock
from googleapiclient.errors import HttpError
from tools.dj_set_processor import helpers


# -----------------------------
# get_shared_filled_fields
# -----------------------------
def test_get_shared_filled_fields_counts_correctly():
    data1 = ["A", "B", ""]
    data2 = ["A", "", "C"]
    indices = [{"index": 0}, {"index": 1}, {"index": 2}]
    assert helpers.get_shared_filled_fields(data1, data2, indices) == 1


# -----------------------------
# get_dedup_match_score
# -----------------------------
def test_get_dedup_match_score_average_similarity():
    data1 = ["Song A", "Remix"]
    data2 = ["Song A", "Remix"]
    indices = [{"index": 0}, {"index": 1}]
    result = helpers.get_dedup_match_score(data1, data2, indices)
    assert 0.9 <= result <= 1.0


def test_get_dedup_match_score_no_common_fields():
    data1 = ["", ""]
    data2 = ["", ""]
    indices = [{"index": 0}, {"index": 1}]
    assert helpers.get_dedup_match_score(data1, data2, indices) == 0


# -----------------------------
# string_similarity & clean_title
# -----------------------------
def test_string_similarity_basic():
    assert helpers.string_similarity("abc", "abc") == 1.0
    assert helpers.string_similarity("abc", "xyz") < 0.5


def test_clean_title_strips_and_lowercases():
    assert helpers.clean_title(" Hello ") == "hello"


# -----------------------------
# hex_to_rgb
# -----------------------------
def test_hex_to_rgb_valid_6char():
    result = helpers.hex_to_rgb("#ff0000")
    assert result == {"red": 1.0, "green": 0.0, "blue": 0.0}


def test_hex_to_rgb_valid_3char():
    result = helpers.hex_to_rgb("#0f0")
    assert pytest.approx(result["green"], 0.003) == 1.0


def test_hex_to_rgb_invalid_returns_white():
    assert helpers.hex_to_rgb("invalid") == {"red": 1.0, "green": 1.0, "blue": 1.0}


# -----------------------------
# _try_lock_folder / _release_folder_lock
# -----------------------------
def test_try_and_release_folder_lock():
    folder_name = "2025"
    result1 = helpers._try_lock_folder(folder_name)
    result2 = helpers._try_lock_folder(folder_name)
    assert result1 is True
    assert result2 is False
    helpers._release_folder_lock(folder_name)
    assert folder_name not in "".join(helpers._folder_locks.keys())


# -----------------------------
# _clean_title
# -----------------------------
def test_clean_title_removes_parentheses():
    assert helpers._clean_title("Song (Remix)") == "Song"
    assert helpers._clean_title("Song") == "Song"


# -----------------------------
# levenshtein_distance / _string_similarity
# -----------------------------
def test_levenshtein_distance_and_string_similarity():
    assert helpers.levenshtein_distance("abc", "abc") == 0
    assert helpers.levenshtein_distance("abc", "abd") == 1
    assert helpers._string_similarity("abc", "abc") == 1.0
    assert 0.0 <= helpers._string_similarity("abc", "xyz") <= 1.0


# -----------------------------
# _get_shared_filled_fields
# -----------------------------
def test_get_shared_filled_fields_private():
    a = ["Song", "Artist", ""]
    b = ["Song", "", "Genre"]
    dedup_indices = [{"index": 0}, {"index": 1}, {"index": 2}]
    assert helpers._get_shared_filled_fields(a, b, dedup_indices) == 1


# -----------------------------
# _get_dedup_match_score
# -----------------------------
def test_get_dedup_match_score_private():
    row_a = ["Song A", "Remix"]
    row_b = ["Song A", "Remix"]
    dedup_indices = [{"field": "Title", "index": 0}, {"field": "Remix", "index": 1}]
    score = helpers._get_dedup_match_score(row_a, row_b, dedup_indices)
    assert 0.9 <= score <= 1.0


def test_get_dedup_match_score_private_with_empty_fields():
    row_a = ["", "Remix"]
    row_b = ["Song", ""]
    dedup_indices = [{"field": "Title", "index": 0}, {"field": "Remix", "index": 1}]
    score = helpers._get_dedup_match_score(row_a, row_b, dedup_indices)
    assert 0 < score <= 1.0


# -----------------------------
# extract_date_and_title
# -----------------------------
def test_extract_date_and_title_valid():
    date, title = helpers.extract_date_and_title("2025-03-29_MyEvent.csv")
    assert date == "2025-03-29"
    assert title.startswith("MyEvent")


def test_extract_date_and_title_invalid():
    date, title = helpers.extract_date_and_title("InvalidName.csv")
    assert date == ""
    assert title == "InvalidName.csv"


# -----------------------------
# try_lock_folder / release_folder_lock (mocked)
# -----------------------------
@mock.patch("core._google_credentials.get_drive_client")
@mock.patch("core.google_drive.get_or_create_subfolder")
def test_try_and_release_lock_drive(mock_get_folder, mock_drive):
    # Setup
    mock_service = mock.Mock()
    mock_drive.return_value = mock_service
    mock_get_folder.return_value = "summary_folder"
    # Simulate no existing locks
    mock_service.files().list().execute.return_value = {"files": []}

    result = helpers.try_lock_folder("2025")
    assert result is True

    # Simulate one existing lock for release
    mock_service.files().list().execute.return_value = {"files": [{"id": "123"}]}
    helpers.release_folder_lock("2025")
    mock_service.files().delete.assert_called_with(fileId="123")


@mock.patch("core._google_credentials.get_drive_client")
@mock.patch("core.google_drive.get_or_create_subfolder")
def test_try_lock_folder_already_locked(mock_get_folder, mock_drive):
    mock_service = mock.Mock()
    mock_drive.return_value = mock_service
    mock_get_folder.return_value = "summary_folder"
    mock_service.files().list().execute.return_value = {"files": [{"id": "lock"}]}
    result = helpers.try_lock_folder("2025")
    assert result is False


@mock.patch("core._google_credentials.get_drive_client")
@mock.patch("core.google_drive.get_or_create_subfolder")
def test_release_folder_lock_handles_http_error(mock_get_folder, mock_drive):
    mock_service = mock.Mock()
    mock_drive.return_value = mock_service
    mock_get_folder.return_value = "summary_folder"
    mock_service.files().list().execute.return_value = {"files": [{"id": "bad"}]}
    mock_service.files().delete.side_effect = HttpError(mock.Mock(), b"Bad Request")

    # Should not raise
    helpers.release_folder_lock("2025")
