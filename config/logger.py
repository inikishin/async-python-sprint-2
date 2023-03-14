import logging
import sys

from constants import LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)

logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
