import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from config import BASE_DIR

_LOGGING_CONFIGURED = False


def setup_logging() -> None:
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    log_dir = BASE_DIR / 'logs'
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / 'app.log'

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
    )

    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler) for h in root.handlers):
        stream = logging.StreamHandler()
        stream.setLevel(logging.INFO)
        stream.setFormatter(formatter)
        root.addHandler(stream)

    if not any(isinstance(h, RotatingFileHandler) and getattr(h, 'baseFilename', '').endswith('app.log') for h in root.handlers):
        file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=7, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    _LOGGING_CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    setup_logging()
    return logging.getLogger(name)
