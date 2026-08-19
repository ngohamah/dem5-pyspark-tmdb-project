"""Writes generated events to CSV files that Spark's file-source stream can pick up.

Files are written to a temporary path and then atomically renamed into the
watched directory. Spark's file stream lists directory contents on every
trigger; a half-written CSV visible mid-write would be read as a partial,
malformed batch. ``os.rename`` within the same filesystem is atomic, so the
watched directory only ever contains complete files.
"""
from __future__ import annotations

import csv
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any

from .config import CSV_COLUMNS, INCOMING_DIR
from .logger_config import configure_logger

logger = configure_logger(__name__)


def ensure_incoming_dir(directory: Path = INCOMING_DIR) -> None:
    directory.mkdir(parents=True, exist_ok=True)


def write_events_csv(events: list[dict[str, Any]], directory: Path = INCOMING_DIR) -> Path:
    """Write ``events`` to a new CSV file in ``directory`` and return its path.

    Returns early without touching the filesystem if ``events`` is empty, so
    callers never end up with a header-only file that Spark would still list.
    """
    if not events:
        logger.warning("No events to write; skipping file creation")
        return None

    ensure_incoming_dir(directory)
    filename = f"events_{uuid.uuid4().hex}.csv"
    final_path = directory / filename

    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".tmp_", suffix=".csv")
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(events)
        os.rename(tmp_path, final_path)
    except Exception:
        logger.exception("Failed to write events CSV; discarding partial file %s", tmp_path)
        Path(tmp_path).unlink(missing_ok=True)
        raise

    logger.info("Wrote %d event(s) to %s", len(events), final_path)
    return final_path
