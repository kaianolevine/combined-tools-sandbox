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
    log.debug("🔐 [get_spotify_client_from_refresh] Called with no parameters.")
    log.debug("🔐 Loading Spotify credentials from environment variables...")

    client_id = config.SPOTIFY_CLIENT_ID
    client_secret = config.SPOTIFY_CLIENT_SECRET
    redirect_uri = config.SPOTIFY_REDIRECT_URI
    refresh_token = config.SPOTIFY_REFRESH_TOKEN

    log.debug(f"[get_spotify_client_from_refresh] Loaded env vars: "
              f"client_id={'set' if client_id else 'unset'}, "
              f"client_secret={'set' if client_secret else 'unset'}, "
              f"redirect_uri={'set' if redirect_uri else 'unset'}, "
              f"refresh_token={'set' if refresh_token else 'unset'}")

    if not all([client_id, client_secret, redirect_uri, refresh_token]):
        log.error("[get_spotify_client_from_refresh] Missing one or more required Spotify credentials.")
        raise ValueError("Missing one or more required Spotify credentials.")

    log.debug("✅ [get_spotify_client_from_refresh] All Spotify environment variables found. Initializing client...")

    auth_manager = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scope="playlist-modify-public playlist-modify-private",
        cache_handler=NoopCacheHandler(),
    )

    log.debug("[get_spotify_client_from_refresh] Refreshing Spotify access token...")
    token_info = auth_manager.refresh_access_token(refresh_token)
    log.info("[get_spotify_client_from_refresh] Obtained new Spotify access token.")
    return Spotify(auth=token_info["access_token"])


def search_track(artist, title):
    log.debug(f"[search_track] Called with artist='{artist}', title='{title}'")
    sp = get_spotify_client_from_refresh()
    query = f"artist:{artist} track:{title}"
    log.debug(f"[search_track] Constructed query: {query}")
    results = sp.search(q=query, type="track", limit=1)

    tracks = results.get("tracks", {}).get("items", [])
    log.debug(f"[search_track] Number of tracks found: {len(tracks)}")
    if tracks:
        log.info(f"[search_track] Found track URI: {tracks[0]['uri']}")
        return tracks[0]["uri"]
    else:
        log.warning("[search_track] No track found for the given artist/title.")
        return None


def add_tracks_to_playlist(uris):
    log.debug(f"[add_tracks_to_playlist] Called with uris={uris}")
    if not config.SPOTIFY_PLAYLIST_ID:
        log.error("[add_tracks_to_playlist] Missing SPOTIFY_PLAYLIST_ID environment variable.")
        raise EnvironmentError("Missing SPOTIFY_PLAYLIST_ID environment variable.")
    if not uris:
        log.info("[add_tracks_to_playlist] No tracks to add.")
        print("No tracks to add.")
        return
    log.debug(f"[add_tracks_to_playlist] Adding {len(uris)} tracks to playlist ID {config.SPOTIFY_PLAYLIST_ID}")
    sp = get_spotify_client_from_refresh()
    sp.playlist_add_items(config.SPOTIFY_PLAYLIST_ID, uris)
    log.info(f"[add_tracks_to_playlist] Added {len(uris)} track(s) to playlist {config.SPOTIFY_PLAYLIST_ID}.")
    print(f"✅ Added {len(uris)} track(s) to playlist.")


def trim_playlist_to_limit(limit=200):
    log.debug(f"[trim_playlist_to_limit] Called with limit={limit}")
    if not config.SPOTIFY_PLAYLIST_ID:
        log.error("[trim_playlist_to_limit] Missing SPOTIFY_PLAYLIST_ID environment variable.")
        raise EnvironmentError("Missing SPOTIFY_PLAYLIST_ID environment variable.")
    sp = get_spotify_client_from_refresh()
    current = sp.playlist_items(
        config.SPOTIFY_PLAYLIST_ID,
        fields="items.track.uri,total",
        additional_types=["track"],
    )
    total = current["total"]
    log.debug(f"[trim_playlist_to_limit] Playlist size: {total}, limit: {limit}")
    if total <= limit:
        log.info(f"[trim_playlist_to_limit] Playlist is within limit ({total}/{limit}); no tracks removed.")
        return
    num_to_remove = total - limit
    uris_to_remove = [item["track"]["uri"] for item in current["items"][:num_to_remove]]
    log.info(f"[trim_playlist_to_limit] Removing {num_to_remove} tracks from playlist ID {config.SPOTIFY_PLAYLIST_ID}.")
    sp.playlist_remove_all_occurrences_of_items(config.SPOTIFY_PLAYLIST_ID, uris_to_remove)
    log.info(f"[trim_playlist_to_limit] Removed {len(uris_to_remove)} old tracks to stay under {limit}.")
    print(f"🗑️ Removed {len(uris_to_remove)} old tracks to stay under {limit}.")
