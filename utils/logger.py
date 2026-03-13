"""
utils/logger.py

Role in pipeline: Cross-cutting concern — imported by every module.
Provides a single get_logger() factory that returns a configured
Python logger writing to both console and logs/migration.log.

Input:  Module name string (pass __name__ from the calling module)
Output: logging.Logger instance ready to use
"""

import logging
import os
from config.settings import get_settings


def get_logger(name: str) -> logging.Logger:
    """
    Return a configured logger for the given module name.

    Writes to both console (stdout) and logs/migration.log simultaneously.
    Guards against duplicate handlers — safe to call multiple times with
    the same name without producing duplicate log lines.

    Args:
        name (str): Module name — pass __name__ from the calling module.
                    Produces log lines like:
                    extractors.mock_extractor | Loaded 20 records

    Returns:
        logging.Logger: Configured logger instance.

    Raises:
        Nothing — logger setup failures are silent by design to avoid
        crashing the pipeline over a logging misconfiguration.
    """
    settings = get_settings()

    # getLogger returns the same object for the same name — Python's
    # logger registry ensures one logger per module name globally.
    logger = logging.getLogger(name)

    # Guard against duplicate handlers.
    # If this logger was already configured (e.g. module imported twice),
    # return it as-is. Without this check, every import adds another handler
    # and every log line prints N times — one per import.
    if logger.handlers:
        return logger

    logger.setLevel(settings.log_level.upper())

    # Consistent format across all modules:
    # 2026-03-13 20:45:01 | INFO     | extractors.mock_extractor | message
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler — visible during pipeline run
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # File handler — permanent record of every pipeline run
    # exist_ok=True prevents crash if logs/ already exists
    os.makedirs("logs", exist_ok=True)
    file_handler = logging.FileHandler("logs/migration.log", encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
