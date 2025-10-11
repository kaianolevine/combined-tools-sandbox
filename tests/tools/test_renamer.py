import os
import pytest
from unittest import mock
from tools.music_tag_sort import renamer


# -----------------------------
# Fixtures
# -----------------------------
@pytest.fixture
def mock_metadata():
    return {
        "artist": "Artist",
        "title": "Title",
        "bpm": "120",
        "comment": "Remix",
        "album": "Album",
        "genre": "Genre",
        "year": "2024",
        "tracknumber": "1",
        "key": "Am",
    }


@pytest.fixture
def mock_config():
    return {
        "rename_order": ["bpm", "title", "artist", "comment"],
        "required_fields": ["title", "artist"],
        "extension": ".mp3",
        "separator": "__",
    }


# -----------------------------
# sanitize_filename / new_sanitize_filename
# -----------------------------
def test_sanitize_filename_basic():
    assert renamer.sanitize_filename("A Song Demo!") == "A_Song_Demo"


def test_new_sanitize_filename_basic():
    assert renamer.new_sanitize_filename("A Song!") == "A_Song_"


# -----------------------------
# get_metadata
# -----------------------------
@mock.patch("tools.music_tag_sort.renamer.MP3")
def test_get_metadata_mp3(mock_mp3):
    mock_audio = mock.Mock()
    mock_audio.tags = {
        "artist": ["Tester"],
        "title": ["Demo"],
        "bpm": ["123.4"],
        "comment": ["Cool Remix"],
        "album": ["Album"],
        "genre": ["Genre"],
        "date": ["2025"],
        "tracknumber": ["1"],
        "initialkey": ["Am"],
    }
    mock_mp3.return_value = mock_audio

    result = renamer.get_metadata("file.mp3")
    assert result["artist"] == "Tester"
    assert result["bpm"] == "123"
    assert result["key"] == "Am"


def test_get_metadata_invalid_format():
    with pytest.raises(ValueError):
        renamer.get_metadata("file.wav")


# -----------------------------
# generate_filename
# -----------------------------
def test_generate_filename_success(mock_metadata, mock_config):
    result = renamer.generate_filename(mock_metadata, mock_config)
    assert result == "120__Title__Artist__Remix.mp3"


def test_generate_filename_missing_required(mock_metadata, mock_config):
    mock_metadata["title"] = ""
    result = renamer.generate_filename(mock_metadata, mock_config)
    assert result is None


def test_generate_filename_no_valid_fields(mock_config):
    result = renamer.generate_filename({}, mock_config)
    assert result is None


# -----------------------------
# _unique_path
# -----------------------------
def test_unique_path_creates_increment(tmp_path):
    base = tmp_path / "file.mp3"
    base.write_text("dummy")
    result = renamer._unique_path(str(base))
    assert "_1.mp3" in result


# -----------------------------
# rename_music_file
# -----------------------------
@mock.patch("tools.music_tag_sort.renamer.get_metadata")
def test_rename_music_file_success(mock_get_metadata, tmp_path):
    mock_get_metadata.return_value = {
        "bpm": "120",
        "title": "Test",
        "artist": "Artist",
        "comment": "Demo",
    }
    file_path = tmp_path / "song.mp3"
    file_path.write_text("test data")

    result = renamer.rename_music_file(str(file_path), str(tmp_path), "__")
    assert os.path.exists(result)
    assert "__Test__Artist__Demo.mp3" in result


# -----------------------------
# rename_files_in_directory
# -----------------------------
@mock.patch("tools.music_tag_sort.renamer.get_metadata")
@mock.patch("tools.music_tag_sort.renamer.generate_filename")
def test_rename_files_in_directory_success(mock_gen, mock_meta, tmp_path):
    mock_meta.return_value = {"artist": "A", "title": "B"}
    mock_gen.return_value = "A__B.mp3"
    file = tmp_path / "file.mp3"
    file.write_text("data")

    summary = renamer.rename_files_in_directory(
        str(tmp_path), {"rename_order": ["artist", "title"]}
    )
    assert summary["renamed"] >= 1


def test_rename_files_in_directory_invalid(tmp_path):
    # Create non-audio file
    f = tmp_path / "not_audio.txt"
    f.write_text("invalid")
    summary = renamer.rename_files_in_directory(str(tmp_path), {"rename_order": ["artist"]})
    assert "processed" in summary


# -----------------------------
# process_drive_folder
# -----------------------------
@mock.patch("tools.music_tag_sort.renamer.rename_music_file")
@mock.patch("tools.music_tag_sort.renamer.drive")
def test_process_drive_folder_success(mock_drive, mock_rename, tmp_path):
    mock_drive.get_drive_service.return_value = "mock_service"
    mock_drive.list_music_files.return_value = [{"name": "test.mp3", "id": "1"}]
    mock_drive.download_file.side_effect = lambda s, fid, dest: open(dest, "w").write("x")
    mock_rename.return_value = tmp_path / "renamed.mp3"

    result = renamer.process_drive_folder("SRC", "DEST", "__")
    assert result["downloaded"] == 1
    assert result["uploaded"] == 1
    assert result["renamed"] == 1


@mock.patch("tools.music_tag_sort.renamer.rename_music_file", side_effect=Exception("bad file"))
@mock.patch("tools.music_tag_sort.renamer.drive")
def test_process_drive_folder_handles_failure(mock_drive, _mock_rename):
    mock_drive.get_drive_service.return_value = "mock_service"
    mock_drive.list_music_files.return_value = [{"name": "broken.mp3", "id": "1"}]
    result = renamer.process_drive_folder("SRC", "DEST", "__")
    assert result["failed"] == 1


# -----------------------------
# Integration: end-to-end filename generation
# -----------------------------
def test_end_to_end_filename_and_rename(tmp_path):
    """Simulate a full rename flow with real file I/O."""
    path = tmp_path / "song.mp3"
    path.write_text("dummy")

    with mock.patch("tools.music_tag_sort.renamer.get_metadata") as mock_meta:
        mock_meta.return_value = {"bpm": "120", "title": "Test", "artist": "Artist", "comment": ""}
        renamed = renamer.rename_music_file(str(path), str(tmp_path), "__")

    assert os.path.exists(renamed)
    assert renamed.endswith(".mp3")
