import logging
import datetime


# Create a shared logger instance
logger = logging.getLogger("core")


# Shortcut aliases
debug = logger.debug
info = logger.info
warning = logger.warning
error = logger.error
exception = logger.exception


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] [%(name)s.%(funcName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_logger():
    return logger


def format_date(dt: datetime.datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")
