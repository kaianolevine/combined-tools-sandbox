from tools.music_tag_sort import renamer


def test_generate_filename_with_all_fields():
    metadata = {"title": "Song", "artist": "Artist", "bpm": "90"}
    config = {"rename_order": ["bpm", "title", "artist"]}
    filename = renamer.generate_filename(metadata, config)
    assert filename == "90__Song__Artist.mp3"


def test_generate_filename_missing_bpm():
    metadata = {"title": "Song", "artist": "Artist"}
    config = {"rename_order": ["bpm", "title", "artist"]}
    filename = renamer.generate_filename(metadata, config)
    assert filename == "Song__Artist.mp3"


def test_generate_filename_missing_title():
    metadata = {"artist": "Artist", "bpm": "90"}
    config = {"rename_order": ["bpm", "title", "artist"]}
    filename = renamer.generate_filename(metadata, config)
    assert filename == "90__Artist.mp3"


def test_generate_filename_missing_artist():
    metadata = {"title": "Song", "bpm": "90"}
    config = {"rename_order": ["bpm", "title", "artist"]}
    filename = renamer.generate_filename(metadata, config)
    assert filename == "90__Song.mp3"


def test_generate_filename_partial_order():
    metadata = {"title": "Song", "artist": "Artist", "bpm": "90"}
    config = {"rename_order": ["title", "artist"]}
    filename = renamer.generate_filename(metadata, config)
    assert filename == "Song__Artist.mp3"


def test_generate_filename_empty_order():
    metadata = {"title": "Song", "artist": "Artist", "bpm": "90"}
    config = {"rename_order": []}
    filename = renamer.generate_filename(metadata, config)
    assert filename is None


def test_generate_filename_with_rename_order_and_logging(caplog):
    metadata = {"title": "Song", "artist": "Artist", "bpm": "90"}
    config = {"rename_order": ["artist", "title"]}
    with caplog.at_level("DEBUG"):
        filename = renamer.generate_filename(metadata, config)
        assert "Generated filename" in caplog.text
        assert filename == "Artist__Song.mp3"
