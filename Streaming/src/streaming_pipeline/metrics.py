"""Streaming query observability: logs and persists per-batch performance data.

Structured Streaming reports progress asynchronously via a
``StreamingQueryListener``. Recording that progress here -- rather than
only printing it -- is what lets performance_metrics.md be generated from
a real run instead of hand-written guesses.
"""
from __future__ import annotations

import csv

from pyspark.sql.streaming import StreamingQueryListener

from .config import METRICS_LOG_PATH
from .logger_config import configure_logger

logger = configure_logger(__name__)

_METRICS_HEADER = [
    "batch_id", "timestamp", "num_input_rows",
    "input_rows_per_second", "processed_rows_per_second", "batch_duration_ms",
]


class MetricsListener(StreamingQueryListener):
    """Appends one row per non-empty micro-batch to logs/batch_metrics.csv."""

    def __init__(self, metrics_path=METRICS_LOG_PATH):
        self.metrics_path = metrics_path
        self._ensure_header()

    def _ensure_header(self) -> None:
        self.metrics_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.metrics_path.exists():
            with self.metrics_path.open("w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(_METRICS_HEADER)

    def onQueryStarted(self, event) -> None:
        logger.info("Streaming query started: id=%s name=%s", event.id, event.name)

    def onQueryProgress(self, event) -> None:
        progress = event.progress
        duration_ms = (progress.durationMs or {}).get("triggerExecution")
        logger.info(
            "Batch %s: input_rows=%d rows/sec=%.2f processed_rows/sec=%.2f duration_ms=%s",
            progress.batchId,
            progress.numInputRows,
            progress.inputRowsPerSecond or 0.0,
            progress.processedRowsPerSecond or 0.0,
            duration_ms,
        )
        if progress.numInputRows == 0:
            return
        with self.metrics_path.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                progress.batchId, progress.timestamp, progress.numInputRows,
                progress.inputRowsPerSecond, progress.processedRowsPerSecond, duration_ms,
            ])

    def onQueryTerminated(self, event) -> None:
        if event.exception:
            logger.error("Streaming query terminated with an error: %s", event.exception)
        else:
            logger.info("Streaming query terminated normally: id=%s", event.id)
