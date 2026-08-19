"""The foreachBatch sink: routes each cleaned micro-batch to Postgres or the
rejected-rows folder.

Structured Streaming calls ``write_micro_batch`` once per micro-batch with a
plain (non-streaming) DataFrame, which is small enough at this scale to
collect to a pandas DataFrame and process with ordinary, testable Python.
"""
from __future__ import annotations

import csv

from pyspark.sql import DataFrame

from .config import BATCH_ROW_COUNTS_PATH, REJECTED_DIR
from .db import INSERT_COLUMNS, insert_events
from .logger_config import configure_logger
from .transform import reduce_reasons_to_counts

logger = configure_logger(__name__)

_ROW_COUNTS_HEADER = ["batch_id", "received", "inserted", "rejected"]


def _record_row_counts(batch_id: int, received: int, inserted: int, rejected: int) -> None:
    BATCH_ROW_COUNTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    is_new_file = not BATCH_ROW_COUNTS_PATH.exists()
    with BATCH_ROW_COUNTS_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if is_new_file:
            writer.writerow(_ROW_COUNTS_HEADER)
        writer.writerow([batch_id, received, inserted, rejected])


def _write_rejected_rows(invalid_rows, batch_id: int) -> None:
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    rejected_path = REJECTED_DIR / f"rejected_batch_{batch_id}.csv"
    invalid_rows.to_csv(rejected_path, index=False)
    counts = reduce_reasons_to_counts(invalid_rows["validation_reason"].tolist())
    logger.warning(
        "Batch %d: dropped %d invalid row(s), reasons=%s -- wrote them to %s",
        batch_id, len(invalid_rows), counts, rejected_path,
    )


def write_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Insert valid rows into Postgres and dump invalid rows for inspection."""
    if batch_df.isEmpty():
        return

    pdf = batch_df.toPandas()
    valid_rows = pdf[pdf["is_valid"]]
    invalid_rows = pdf[~pdf["is_valid"]]

    inserted = 0
    if not valid_rows.empty:
        records = valid_rows[list(INSERT_COLUMNS)].to_dict("records")
        inserted = insert_events(records)

    if not invalid_rows.empty:
        _write_rejected_rows(invalid_rows, batch_id)

    _record_row_counts(batch_id, len(pdf), inserted, len(invalid_rows))
    logger.info(
        "Batch %d complete: %d row(s) received, %d inserted, %d rejected",
        batch_id, len(pdf), inserted, len(invalid_rows),
    )
