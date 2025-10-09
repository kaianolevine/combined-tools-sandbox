# tests/test_renamer.py

import os
import pytest
from unittest import mock
import tools.music_tag_sort.renamer as renamer


def test_sanitize_filename_and_new_sanitize_filename():
    assert renamer.sanitize_filename("Hello World!") == "Hello_World"
    assert renamer.new_sanitize_filename("A (song) [demo]!").startswith("A__song")
    assert "demo" in renamer.new_sanitize_filename("A (song) [demo]!")


def test_generate_filename_with_all_fields():
    metadata = {"bpm": "120", "title": "Song", "artist": "Artist", "comment": "Live"}
    config = {"rename_order": ["bpm", "title", "artist"], "extension": ".mp3"}
    filename = renamer.generate_filename(metadata, config)
    assert filename.startswith("120__Song__Artist")
    assert filename.endswith(".mp3")


def test_generate_filename_missing_required_field():
    metadata = {"bpm": "", "title": "Song", "artist": "Artist"}
    config = {
        "rename_order": ["bpm", "title", "artist"],
        "required_fields": ["bpm"],
        "extension": ".mp3",
    }
    assert renamer.generate_filename(metadata, config) is None


def test_generate_filename_all_empty():
    metadata = {}
    config = {"rename_order": ["title", "artist"], "extension": ".mp3"}
    assert renamer.generate_filename(metadata, config) is None


def test_get_metadata_mp3(monkeypatch):
    fake_audio = mock.Mock()
    fake_audio.tags = {"artist": ["Me"], "title": ["Song"], "bpm": ["128.2"], "comment": ["ok"]}
    monkeypatch.setattr(renamer, "MP3", lambda f, ID3=None: fake_audio)
    result = renamer.get_metadata("track.mp3")
    assert result["artist"] == "Me"
    assert result["bpm"] == "128"


def test_get_metadata_invalid(monkeypatch):
    with pytest.raises(ValueError):
        renamer.get_metadata("file.xyz")


def test_rename_music_file(monkeypatch, tmp_path):
    file_path = tmp_path / "old.mp3"
    file_path.write_text("fake")

    metadata = {"bpm": "120", "title": "Song", "artist": "Artist", "comment": ""}
    monkeypatch.setattr(renamer, "get_metadata", lambda f: metadata)

    monkeypatch.setattr(renamer, "sanitize_filename", lambda v: v)
    monkeypatch.setattr(os, "rename", lambda src, dst: dst)

    output_dir = tmp_path
    new_path = renamer.rename_music_file(str(file_path), str(output_dir))
    assert "120__Song__Artist" in new_path


def test_rename_files_in_directory(monkeypatch, tmp_path):
    file_path = tmp_path / "song.mp3"
    file_path.write_text("fake")

    metadata = {"title": "Title", "artist": "Artist", "bpm": "100"}
    monkeypatch.setattr(renamer, "get_metadata", lambda f: metadata)
    monkeypatch.setattr(renamer, "sanitize_filename", lambda v: v)
    monkeypatch.setattr(os, "rename", lambda src, dst: None)

    renamer.rename_files_in_directory(str(tmp_path), {"rename_order": ["bpm", "title", "artist"], "extension": ".mp3"})


def test_rename_files_in_directory_skips_missing(monkeypatch, tmp_path):
    file_path = tmp_path / "song.mp3"
    file_path.write_text("fake")

    metadata = {"title": "", "artist": ""}
    monkeypatch.setattr(renamer, "get_metadata", lambda f: metadata)
    monkeypatch.setattr(renamer, "generate_filename", lambda m, c: None)

    renamer.rename_files_in_directory(str(tmp_path), {"rename_order": ["title"], "extension": ".mp3"})