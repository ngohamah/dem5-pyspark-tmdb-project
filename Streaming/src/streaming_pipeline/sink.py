"""The foreachBatch sink: routes each cleaned micro-batch to Postgres or the
rejected-rows folder.

Each partition of the micro-batch is processed independently via
``batch_df.rdd.mapPartitions`` -- on whichever executor already holds that
partition's data -- rather than collecting the whole batch onto the driver
with ``toPandas()`` first. Only a small per-partition (received, inserted,
rejected) summary is ever collected back to the driver, so batch size is
bounded by executor memory, not driver memory.

``rejected_dir``/``row_counts_path`` are threaded through as real function
arguments rather than read from module-level constants inside the
partition-processing code. Spark always runs ``mapPartitions`` functions in
a separate Python worker process (even under a single-core local session),
which re-imports this module fresh -- so a test's ``monkeypatch.setattr``
on a module attribute would silently have no effect there. Passing the
paths as arguments instead means the actual value gets captured in the
closure and shipped to the worker, so it's honored correctly in both
production and tests.
"""
from __future__ import annotations

import csv
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from pyspark import TaskContext
from pyspark.sql import DataFrame

from .config import BATCH_ROW_COUNTS_PATH, REJECTED_DIR
from .db import INSERT_COLUMNS, insert_events
from .logger_config import configure_logger
from .transform import reduce_reasons_to_counts

logger = configure_logger(__name__)

_ROW_COUNTS_HEADER = ["batch_id", "received", "inserted", "rejected"]


def _record_row_counts(row_counts_path: Path, batch_id: int, received: int, inserted: int, rejected: int) -> None:
    row_counts_path.parent.mkdir(parents=True, exist_ok=True)
    is_new_file = not row_counts_path.exists()
    with row_counts_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if is_new_file:
            writer.writerow(_ROW_COUNTS_HEADER)
        writer.writerow([batch_id, received, inserted, rejected])


def _rejected_path(rejected_dir: Path, batch_id: int, partition_id: int) -> Path:
    return rejected_dir / f"rejected_batch_{batch_id}_part_{partition_id}.csv"


def _write_rejected_rows(
    rejected_rows: list[dict[str, Any]], batch_id: int, partition_id: int, rejected_dir: Path,
) -> None:
    rejected_dir.mkdir(parents=True, exist_ok=True)
    rejected_path = _rejected_path(rejected_dir, batch_id, partition_id)
    fieldnames = list(rejected_rows[0].keys())
    with rejected_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rejected_rows)
    counts = reduce_reasons_to_counts([row["validation_reason"] for row in rejected_rows])
    logger.warning(
        "Batch %d partition %d: dropped %d invalid row(s), reasons=%s -- wrote them to %s",
        batch_id, partition_id, len(rejected_rows), counts, rejected_path,
    )


def process_partition(
    rows: Iterable[dict[str, Any]],
    batch_id: int,
    partition_id: int,
    insert_fn: Callable[[list[dict[str, Any]]], int] = insert_events,
    rejected_dir: Path = REJECTED_DIR,
) -> tuple[int, int, int]:
    """Insert one partition's valid rows into Postgres and save its invalid
    rows to their own rejected-rows file.

    ``rows`` is any iterable of dict-like records (each with every column in
    ``INSERT_COLUMNS`` plus ``is_valid``/``validation_reason``). This is
    ordinary Python with no Spark dependency, so it can be called directly --
    with a stub ``insert_fn`` -- from a unit test without starting a Spark
    job. In production it runs once per partition inside
    ``batch_df.rdd.mapPartitions``, on whichever executor already holds that
    data.

    Returns ``(received, inserted, rejected)`` row counts for this partition.
    """
    received = 0
    valid_records = []
    rejected_rows = []

    for row in rows:
        received += 1
        if row["is_valid"]:
            valid_records.append({column: row[column] for column in INSERT_COLUMNS})
        else:
            rejected_rows.append(row)

    inserted = insert_fn(valid_records) if valid_records else 0

    if rejected_rows:
        _write_rejected_rows(rejected_rows, batch_id, partition_id, rejected_dir)

    return received, inserted, len(rejected_rows)


def write_micro_batch(
    batch_df: DataFrame,
    batch_id: int,
    rejected_dir: Path = REJECTED_DIR,
    row_counts_path: Path = BATCH_ROW_COUNTS_PATH,
) -> None:
    """Insert valid rows into Postgres and dump invalid rows for inspection."""
    if batch_df.isEmpty():
        return

    def _run_partition(rows):
        partition_id = TaskContext.get().partitionId()
        row_dicts = (row.asDict() for row in rows)
        yield process_partition(row_dicts, batch_id, partition_id, rejected_dir=rejected_dir)

    summaries = batch_df.rdd.mapPartitions(_run_partition).collect()
    received = sum(s[0] for s in summaries)
    inserted = sum(s[1] for s in summaries)
    rejected = sum(s[2] for s in summaries)

    _record_row_counts(row_counts_path, batch_id, received, inserted, rejected)
    logger.info(
        "Batch %d complete: %d row(s) received, %d inserted, %d rejected",
        batch_id, received, inserted, rejected,
    )
