import logging
import os
from pathlib import Path

# ensure logs folder exists
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "etl_log.txt"


def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # prevent adding multiple handlers if logger is reused
    if not logger.handlers:
        file_handler = logging.FileHandler(LOG_FILE)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | PID:%(process)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S.%f",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
