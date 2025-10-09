# tests/test_core_m3u_parsing.py

import io
from unittest import mock

import core.m3u_parsing as m3u_parsing


def test_parse_time_str_valid():
    assert m3u_parsing.parse_time_str("01:30") == 90
    assert m3u_parsing.parse_time_str("00:05") == 5


def test_parse_time_str_invalid():
    assert m3u_parsing.parse_time_str("bad") == 0
    assert m3u_parsing.parse_time_str("") == 0


def test_extract_tag_value_found_and_not_found():
    line = "<time>12:34</time><title>Song</title>"
    assert m3u_parsing.extract_tag_value(line, "time") == "12:34"
    assert m3u_parsing.extract_tag_value(line, "title") == "Song"
    assert m3u_parsing.extract_tag_value(line, "artist") == ""


def test_get_most_recent_m3u_file_no_files(monkeypatch):
    service = mock.Mock()
    service.files().list().execute.return_value = {"files": []}
    result = m3u_parsing.get_most_recent_m3u_file(service)
    assert result is None


def test_get_most_recent_m3u_file_with_files(monkeypatch):
    files = [{"id": "1", "name": "2024-01-01.m3u"}, {"id": "2", "name": "2025-01-01.m3u"}]
    service = mock.Mock()
    service.files().list().execute.return_value = {"files": files}
    result = m3u_parsing.get_most_recent_m3u_file(service)
    assert result["id"] == "2"
    assert result["name"].endswith(".m3u")


def test_download_m3u_file(monkeypatch):
    # Fake drive service and downloader
    service = mock.Mock()
    request = mock.Mock()
    service.files().get_media.return_value = request

    class FakeDownloader:
        def __init__(self, fh, req):
            self._calls = 0

        def next_chunk(self):
            self._calls += 1
            if self._calls == 1:
                progress = mock.Mock()
                progress.progress.return_value = 0.5
                return progress, False
            else:
                progress = mock.Mock()
                progress.progress.return_value = 1.0
                return progress, True

    monkeypatch.setattr(m3u_parsing, "MediaIoBaseDownload", FakeDownloader)

    # Prepare fake file data
    fh = io.BytesIO("line1\nline2".encode("utf-8"))
    monkeypatch.setattr(io, "BytesIO", lambda: fh)

    lines = m3u_parsing.download_m3u_file(service, "fileid")
    assert lines == ["line1", "line2"]


def test_parse_m3u_lines_basic(monkeypatch):
    lines = [
        "#EXTVDJ:<time>00:01</time><title>Song1</title><artist>Artist1</artist>",
        "#EXTVDJ:<time>23:59</time><title>Song2</title><artist>Artist2</artist>",
        "#EXTVDJ:<time>00:00</time><title>Song3</title><artist>Artist3</artist>",  # rollover
    ]
    existing_keys = set()
    entries = m3u_parsing.parse_m3u_lines(lines, existing_keys, "2025-01-01")

    assert len(entries) == 3
    # Ensure rollover happened: third entry date is next day
    assert entries[2][0].startswith("2025-01-02")
    assert entries[0][1] == "Song1"
    assert entries[1][2] == "Artist2"


def test_parse_m3u_lines_skips_duplicates():
    lines = [
        "#EXTVDJ:<time>00:01</time><title>Song1</title><artist>Artist1</artist>",
        "#EXTVDJ:<time>00:01</time><title>Song1</title><artist>Artist1</artist>",  # duplicate
    ]
    existing_keys = set()
    entries = m3u_parsing.parse_m3u_lines(lines, existing_keys, "2025-01-01")
    assert len(entries) == 1
    assert entries[0][1] == "Song1"
