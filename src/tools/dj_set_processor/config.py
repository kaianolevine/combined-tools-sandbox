import logging

# === CONFIGURATION ===
CSV_SOURCE_FOLDER_ID = "1t4d_8lMC3ZJfSyainbpwInoDta7n69hC"
DJ_SETS_FOLDER_ID = "1A0tKQ2DBXI1Bt9h--olFwnBNne3am-rL"
SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = "credentials.json"

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

logger = logging.getLogger(__name__)

SUMMARY_TAB_NAME = "Summary"
TEMP_TAB_NAME = "TempClear"
OUTPUT_NAME = "DJ Set Collection"

# Constants (replace with your actual folder ID and scopes)
DJ_SETS = "1A0tKQ2DBXI1Bt9h--olFwnBNne3am-rL"
SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = "credentials.json"
