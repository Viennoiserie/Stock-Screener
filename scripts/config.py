import pytz
import logging

from pathlib import Path
from ib_insync import IB

# === Logging Configuration === #

LOG_LEVEL = logging.DEBUG
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# === Miscellaneous Constants === #

OUTPUT_DIR = Path("../output")
RESULTS_FILE = OUTPUT_DIR / "screener_results.txt"

MAX_TICKERS = 50
ICON_PATH = "money_analyze_icon_143358.ico"

# === GUI Defaults === #

APP_TITLE = "Nasdaq Stock Screener"
DEFAULT_FULLSCREEN = True
DEFAULT_ZOOMED = True

# === Interactive Brokers Configuration === #

EASTERN_TZ = pytz.timezone("US/Eastern")
IB_GATEWAY_HOST = '127.0.0.1'
IB_GATEWAY_PORT = 7497
IB_CLIENT_ID = 1
