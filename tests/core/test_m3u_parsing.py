import pytest
from unittest import mock
from core import m3u_parsing


@pytest.fixture
def mock_drive_service():
    return mock.MagicMock()


# ---- parse_time_str ----


def test_parse_time_str_valid():
    """Ensure valid HH:MM strings are parsed correctly."""
    result = m3u_parsing.parse_time_str("01:30")
    assert result == 90


def test_parse_time_str_invalid(caplog):
    result = m3u_parsing.parse_time_str("bad")
    assert result == 0
    assert "Error parsing time string" in caplog.text


# ---- extract_tag_value ----


def test_extract_tag_value_found():
    line = "<artist>Daft Punk</artist>"
    assert m3u_parsing.extract_tag_value(line, "artist") == "Daft Punk"


def test_extract_tag_value_not_found():
    line = "<title>Something</title>"
    assert m3u_parsing.extract_tag_value(line, "artist") == ""


# ---- get_most_recent_m3u_file ----


def test_get_most_recent_m3u_file_success(monkeypatch, mock_drive_service):
    mock_drive_service.files.return_value.list.return_value.execute.return_value = {
        "files": [{"id": "1", "name": "2024-01-01.m3u"}, {"id": "2", "name": "2024-01-02.m3u"}]
    }
    monkeypatch.setattr(m3u_parsing.config, "FOLDER_ID", "folder123")

    result = m3u_parsing.get_most_recent_m3u_file(mock_drive_service)
    assert result["name"] == "2024-01-02.m3u"


def test_get_most_recent_m3u_file_empty(monkeypatch, mock_drive_service):
    mock_drive_service.files.return_value.list.return_value.execute.return_value = {"files": []}
    monkeypatch.setattr(m3u_parsing.config, "FOLDER_ID", "folder123")

    result = m3u_parsing.get_most_recent_m3u_file(mock_drive_service)
    assert result is None


# ---- download_m3u_file ----


def test_download_m3u_file_success(monkeypatch, mock_drive_service):
    """Ensure file download completes successfully even if content is empty."""
    fake_downloader = mock.MagicMock()
    progress = mock.Mock()
    progress.progress.return_value = 1.0
    fake_downloader.next_chunk.side_effect = [(progress, True)]

    monkeypatch.setattr("core.m3u_parsing.MediaIoBaseDownload", lambda fh, req: fake_downloader)
    mock_drive_service.files.return_value.get_media.return_value = mock.Mock()

    # The function should run without raising errors.
    result = m3u_parsing.download_m3u_file(mock_drive_service, "file123")
    assert result is None or isinstance(result, list)


def test_download_m3u_file_partial_progress(monkeypatch, mock_drive_service):
    fake_downloader = mock.MagicMock()
    s1, s2 = mock.Mock(), mock.Mock()
    s1.progress.return_value = 0.5
    s2.progress.return_value = 1.0
    fake_downloader.next_chunk.side_effect = [(s1, False), (s2, True)]
    monkeypatch.setattr("core.m3u_parsing.MediaIoBaseDownload", lambda fh, req: fake_downloader)
    mock_drive_service.files.return_value.get_media.return_value = mock.Mock()

    lines = m3u_parsing.download_m3u_file(mock_drive_service, "file123")
    assert isinstance(lines, list)


# ---- parse_m3u_lines ----


def test_parse_m3u_lines_single_entry(monkeypatch):
    monkeypatch.setattr(m3u_parsing.config, "TIMEZONE", "UTC")
    lines = [
        "#EXTVDJ:<time>12:00</time><title>Track 1</title><artist>Artist</artist>",
    ]
    result = m3u_parsing.parse_m3u_lines(lines, set(), "2025-01-01")
    assert len(result) == 1
    assert "Track 1" in result[0]


def test_parse_m3u_lines_rollover(monkeypatch):
    monkeypatch.setattr(m3u_parsing.config, "TIMEZONE", "UTC")
    lines = [
        "#EXTVDJ:<time>23:59</time><title>Old</title><artist>A</artist>",
        "#EXTVDJ:<time>00:01</time><title>New</title><artist>A</artist>",
    ]
    result = m3u_parsing.parse_m3u_lines(lines, set(), "2025-01-01")
    # Should detect rollover and have 2 entries with increasing dates
    assert len(result) == 2
    assert "2025-01-02" in result[1][0]


def test_parse_m3u_lines_skips_duplicates(monkeypatch):
    monkeypatch.setattr(m3u_parsing.config, "TIMEZONE", "UTC")
    existing = set()
    lines = [
        "#EXTVDJ:<time>12:00</time><title>Song</title><artist>Artist</artist>",
        "#EXTVDJ:<time>12:00</time><title>Song</title><artist>Artist</artist>",
    ]
    result = m3u_parsing.parse_m3u_lines(lines, existing, "2025-01-01")
    assert len(result) == 1


def test_parse_m3u_lines_missing_tags(monkeypatch):
    monkeypatch.setattr(m3u_parsing.config, "TIMEZONE", "UTC")
    lines = ["#EXTVDJ:<time></time><title></title>"]
    result = m3u_parsing.parse_m3u_lines(lines, set(), "2025-01-01")
    assert result == []


# ---- parse_m3u ----


def test_parse_m3u_valid(tmp_path):
    m3u_path = tmp_path / "sample.m3u"
    content = "#EXTVDJ:<artist>ABBA</artist><title>Dancing Queen</title>"
    m3u_path.write_text(content)
    fake_sheets = mock.MagicMock()
    result = m3u_parsing.parse_m3u(fake_sheets, str(m3u_path), "sheet123")
    assert len(result) == 1
    assert result[0][0] == "ABBA"


def test_parse_m3u_missing_tags(tmp_path):
    m3u_path = tmp_path / "invalid.m3u"
    content = "#EXTVDJ:<title>Song</title>"
    m3u_path.write_text(content)
    fake_sheets = mock.MagicMock()
    result = m3u_parsing.parse_m3u(fake_sheets, str(m3u_path), "sid")
    assert result == []


def test_parse_m3u_ignores_non_vdj_lines(tmp_path):
    m3u_path = tmp_path / "mixed.m3u"
    content = "Random line\n#EXTVDJ:<artist>A</artist><title>T</title>"
    m3u_path.write_text(content)
    fake_sheets = mock.MagicMock()
    result = m3u_parsing.parse_m3u(fake_sheets, str(m3u_path), "id")
    assert len(result) == 1
    assert result[0][0] == "A"
