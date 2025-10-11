# tests/test_core_spotify.py

import pytest
from unittest import mock
import core.spotify as spotify
import config


def test_get_spotify_client_from_refresh_success(monkeypatch):
    # Patch config values
    monkeypatch.setattr(config, "SPOTIFY_CLIENT_ID", "id")
    monkeypatch.setattr(config, "SPOTIFY_CLIENT_SECRET", "secret")
    monkeypatch.setattr(config, "SPOTIFY_REDIRECT_URI", "uri")
    monkeypatch.setattr(config, "SPOTIFY_REFRESH_TOKEN", "refresh")

    fake_auth = mock.Mock()
    fake_auth.refresh_access_token.return_value = {"access_token": "tok"}
    monkeypatch.setattr(spotify, "SpotifyOAuth", lambda **kwargs: fake_auth)
    monkeypatch.setattr(spotify, "Spotify", lambda auth: f"SpotifyClient({auth})")

    client = spotify.get_spotify_client_from_refresh()
    assert client == "SpotifyClient(tok)"


def test_get_spotify_client_from_refresh_missing(monkeypatch):
    monkeypatch.setattr(config, "SPOTIFY_CLIENT_ID", None)
    monkeypatch.setattr(config, "SPOTIFY_CLIENT_SECRET", "secret")
    monkeypatch.setattr(config, "SPOTIFY_REDIRECT_URI", "uri")
    monkeypatch.setattr(config, "SPOTIFY_REFRESH_TOKEN", "refresh")
    with pytest.raises(ValueError):
        spotify.get_spotify_client_from_refresh()


def test_search_track_found(monkeypatch):
    fake_sp = mock.Mock()
    fake_sp.search.return_value = {"tracks": {"items": [{"uri": "spotify:track:123"}]}}
    monkeypatch.setattr(spotify, "get_spotify_client_from_refresh", lambda: fake_sp)

    uri = spotify.search_track("Artist", "Title")
    assert uri == "spotify:track:123"


def test_search_track_not_found(monkeypatch):
    fake_sp = mock.Mock()
    fake_sp.search.return_value = {"tracks": {"items": []}}
    monkeypatch.setattr(spotify, "get_spotify_client_from_refresh", lambda: fake_sp)

    uri = spotify.search_track("NoArtist", "NoTitle")
    assert uri is None


def test_add_tracks_to_playlist_success(monkeypatch, capsys):
    monkeypatch.setattr(config, "SPOTIFY_PLAYLIST_ID", "playlist123")
    fake_sp = mock.Mock()
    monkeypatch.setattr(spotify, "get_spotify_client_from_refresh", lambda: fake_sp)

    spotify.add_tracks_to_playlist(["uri1", "uri2"])
    fake_sp.playlist_add_items.assert_called_once_with("playlist123", ["uri1", "uri2"])
    out, _ = capsys.readouterr()
    assert "✅ Added 2 track(s)" in out


def test_add_tracks_to_playlist_missing_id(monkeypatch):
    monkeypatch.setattr(config, "SPOTIFY_PLAYLIST_ID", None)
    with pytest.raises(EnvironmentError):
        spotify.add_tracks_to_playlist(["uri"])


def test_add_tracks_to_playlist_empty(monkeypatch, capsys):
    monkeypatch.setattr(config, "SPOTIFY_PLAYLIST_ID", "playlist123")
    spotify.add_tracks_to_playlist([])
    out, _ = capsys.readouterr()
    assert "No tracks to add." in out


def test_trim_playlist_to_limit_within_limit(monkeypatch):
    monkeypatch.setattr(config, "SPOTIFY_PLAYLIST_ID", "playlist123")
    fake_sp = mock.Mock()
    fake_sp.playlist_items.return_value = {"items": [], "total": 5}
    monkeypatch.setattr(spotify, "get_spotify_client_from_refresh", lambda: fake_sp)

    spotify.trim_playlist_to_limit(limit=10)
    fake_sp.playlist_remove_all_occurrences_of_items.assert_not_called()


def test_trim_playlist_to_limit_exceeds(monkeypatch, capsys):
    monkeypatch.setattr(config, "SPOTIFY_PLAYLIST_ID", "playlist123")
    fake_sp = mock.Mock()
    fake_sp.playlist_items.return_value = {
        "items": [
            {"track": {"uri": "uri1"}},
            {"track": {"uri": "uri2"}},
            {"track": {"uri": "uri3"}},
        ],
        "total": 3,
    }
    monkeypatch.setattr(spotify, "get_spotify_client_from_refresh", lambda: fake_sp)

    spotify.trim_playlist_to_limit(limit=1)
    fake_sp.playlist_remove_all_occurrences_of_items.assert_called_once_with(
        "playlist123", ["uri1", "uri2"]
    )
    out, _ = capsys.readouterr()
    assert "🗑️ Removed 2 old tracks" in out


def test_trim_playlist_to_limit_missing_id(monkeypatch):
    monkeypatch.setattr(config, "SPOTIFY_PLAYLIST_ID", None)
    with pytest.raises(EnvironmentError):
        spotify.trim_playlist_to_limit(limit=5)
