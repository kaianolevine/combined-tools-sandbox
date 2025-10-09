from __future__ import print_function
from tools.live_music_history import m3u_parsing
from core import logger as log

log = log.get_logger()


if __name__ == "__main__":
    m3u_parsing.parse_m3u_and_insert_to_sheet()
