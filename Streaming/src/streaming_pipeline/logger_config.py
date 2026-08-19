"""Centralized logging setup for the streaming pipeline."""
from __future__ import annotations

import logging

from .config import LOG_DIR


def configure_logger(name: str = "streaming_pipeline") -> logging.Logger:
    """Create and return a logger that writes to logs/pipeline.log."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        file_handler = logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        logger.addHandler(file_handler)

    return logger
