import os
from dotenv import load_dotenv

# Load from .env if it exists (useful for local development)
load_dotenv()

# Folders and Spreadsheets IDs and Names
# DJ_SETS = "11zVwUZLDfB6uXpwNdA3c4Xsev2aG26fc"  # My Drive/Deejay Marvel/DJ Sets
# CSV_FILES = "1YskZ8sD2H0bA9rxzWnE8iV15P7kWRk8N"  # My Drive/Deejay Marvel/CSV-Uploaded

CSV_SOURCE_FOLDER_ID = "1t4d_8lMC3ZJfSyainbpwInoDta7n69hC"  # KaianoLevineWCS/CSV
DJ_SETS_FOLDER_ID_OLD = "11zVwUZLDfB6uXpwNdA3c4Xsev2aG26fc"  # My Drive/Deejay Marvel/DJ Sets
DJ_SETS_FOLDER_ID = "1A0tKQ2DBXI1Bt9h--olFwnBNne3am-rL"  # KaianoLevineWCS/DJ Sets

MUSIC_UPLOAD_SOURCE_FOLDER_ID = "1Iu5TwzOXVqCDef2X8S5TZcFo1NdSHpRU"  # Music tagging input
MUSIC_TAGGING_OUTPUT_FOLDER_ID = "1unEJWqYnmiQ3MbaeuBtuxqvz4FJSXBAL"  # Music tagging output
SEP_CHARACTERS = "__"

VDJ_HISTORY_FOLDER_ID = "1FzuuO3xmL2n-8pZ_B-FyrvGWaLxLED3o"  # VDJ/History
LIVE_HISTORY_SPREADSHEET_ID = (
    "1DpUCQWK3vGGdzUC5JmXVeojqsM_hp7U2DcSEGq6cF-U"  # Deejay Marvel - Last 3 Hours
)

# Spotify configuration
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "407fc37efd5f413b9c9ac3f516875d5b")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "31a370bc67854a16b0ee28a2694e17d2")
SPOTIFY_REFRESH_TOKEN = os.getenv(
    "SPOTIFY_REFRESH_TOKEN",
    "AQBS86HeUsmZXZAMtzmMAsMYGFnPNkjzGCx5OwgGSjnKQ_Tvc9g_HGU0aTVhCX5BURzfJ9yRReoe44p96TKEusRZHUlq_IAi5cm8Go0CKqm3XKtJSUNVubwzwZM0KSNJjes",
)
SPOTIFY_REDIRECT_URI = "http://127.0.0.1:8888/callback"
SPOTIFY_USERNAME = "31oya3ie2f5wwlqt6tnfurou6zzq"  # Deejey Marvel Automations
SPOTIFY_PLAYLIST_ID = "5UgPYNOMpFXzuqzDJeervs"  # TestPlaylist

HISTORY_TO_SPOTIFY_LOGGING = os.getenv(
    "SPREADSHEET_ID", "1PLgk1Cbg9C6eJqoQX0U219X-sy3989PhNbhLQCWp54I"
)  # New Drive / logging
# LOG_SHEET_NAME = os.getenv("LOG_SHEET_NAME", "Westie Radio Log")
# HISTORY_TO_SPOTIFY_LOGGING = os.getenv("SPREADSHEET_ID", "11b2haIHc6l2Y8iSggrcjBcSG8dhEbL1c9n3bRJMZORU") #Pipelines/New Logging
# HISTORY_TO_SPOTIFY_LOGGING = os.getenv("SPREADSHEET_ID", "1OdYmoMNOPSD2JiRcr5tf-go_Xzp7_0-pmJ8r6zYD_uc") #Pipelines/Logging

# --- CONFIG --- recent history
HISTORY_IN_HOURS = 3
NO_HISTORY = "No_recent_history_found"
TIMEZONE = "America/Chicago"  # adjust as needed


# === CONFIGURATION === DJ Sets
ALLOWED_HEADERS = ["title", "artist", "remix", "comment", "genre", "length", "bpm", "year"]
desiredOrder = ["Title", "Remix", "Artist", "Comment", "Genre", "Year", "BPM", "Length"]
SUMMARY_FOLDER_NAME = "Summary"
SUMMARY_TAB_NAME = "Summary_Tab"
LOCK_FILE_NAME = ".lock_summary_folder"
TEMP_TAB_NAME = "TempClear"
OUTPUT_NAME = "DJ Set Collection"
ARCHIVE_FOLDER_NAME = "csvs"
