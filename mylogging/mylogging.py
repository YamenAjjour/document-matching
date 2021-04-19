import logging
from datetime import datetime

def setup_logging(path):
    logging.basicConfig(filename=path, level=logging.DEBUG,format='%(message)s')
    logging.warning("\n")
    logging.warning(datetime.now())
    logging.warning("==========================\n")

def log_message(message):
    logging.warning(message)
