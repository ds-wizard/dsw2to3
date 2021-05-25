import logging
import sys

from dsw2to3.consts import LOGGER_NAME


logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(message)s',
)
LOGGER = logging.getLogger(LOGGER_NAME)
