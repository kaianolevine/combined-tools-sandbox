from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth, CacheHandler
from tools.westie_radio import config
from core import logger as log

log = log.get_logger()


class NoopCacheHandler(CacheHandler):
    def get_cached_token(self):
        return None

    def save_token_to_cache(self, token_info):
        pass


def get_spotify_client_from_refresh() -> Spotify:
    log.debug("🔐 Loading Spotify credentials from environment variables...")

    client_id = config.SPOTIFY_CLIENT_ID
    client_secret = config.SPOTIFY_CLIENT_SECRET
    redirect_uri = config.SPOTIFY_REDIRECT_URI
    refresh_token = config.SPOTIFY_REFRESH_TOKEN

    if not all([client_id, client_secret, redirect_uri, refresh_token]):
        raise ValueError("Missing one or more required Spotify credentials.")

    log.debug("✅ All Spotify environment variables found. Initializing client...")

    auth_manager = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scope="playlist-modify-public playlist-modify-private",
        cache_handler=NoopCacheHandler(),
    )

    token_info = auth_manager.refresh_access_token(refresh_token)
    return Spotify(auth=token_info["access_token"])


def search_track(artist, title):
    log.debug(f"🔍 Searching for track: Artist='{artist}', Title='{title}'")
    sp = get_spotify_client_from_refresh()
    query = f"artist:{artist} track:{title}"
    results = sp.search(q=query, type="track", limit=1)

    tracks = results.get("tracks", {}).get("items", [])
    if tracks:
        log.debug(f"✅ Found track URI: {tracks[0]['uri']}")
        return tracks[0]["uri"]
    else:
        log.debug("❌ No track found")
        return None


def add_tracks_to_playlist(uris):
    if not config.SPOTIFY_PLAYLIST_ID:
        raise EnvironmentError("Missing SPOTIFY_PLAYLIST_ID environment variable.")
    if not uris:
        print("No tracks to add.")
        return
    sp = get_spotify_client_from_refresh()
    sp.playlist_add_items(config.SPOTIFY_PLAYLIST_ID, uris)
    print(f"✅ Added {len(uris)} track(s) to playlist.")


def trim_playlist_to_limit(limit=200):
    if not config.SPOTIFY_PLAYLIST_ID:
        raise EnvironmentError("Missing SPOTIFY_PLAYLIST_ID environment variable.")
    sp = get_spotify_client_from_refresh()
    current = sp.playlist_items(
        config.SPOTIFY_PLAYLIST_ID,
        fields="items.track.uri,total",
        additional_types=["track"],
    )
    total = current["total"]
    if total <= limit:
        return
    uris_to_remove = [item["track"]["uri"] for item in current["items"][: total - limit]]
    sp.playlist_remove_all_occurrences_of_items(config.SPOTIFY_PLAYLIST_ID, uris_to_remove)
    print(f"🗑️ Removed {len(uris_to_remove)} old tracks to stay under {limit}.")
