import logging

# === CONFIGURATION ===
CSV_SOURCE_FOLDER_ID = "1t4d_8lMC3ZJfSyainbpwInoDta7n69hC"
DJ_SETS_FOLDER_ID = "1A0tKQ2DBXI1Bt9h--olFwnBNne3am-rL"
DJ_SETS = DJ_SETS_FOLDER_ID

SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = "credentials.json"

ALLOWED_HEADERS = ["title", "artist", "remix", "comment", "genre", "length", "bpm", "year"]

SUMMARY_FOLDER_NAME = "Summary"
LOCK_FILE_NAME = ".lock_summary_folder"
TEMP_TAB_NAME = "TempClear"
OUTPUT_NAME = "DJ Set Collection"

# Set up logging
# logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
