"""Tests for src/streaming_pipeline/sink.py.

``process_partition`` is plain Python with injectable ``insert_fn``/
``rejected_dir`` arguments, so it's tested directly here with Postgres
stubbed out -- no Spark job runs. That's a deliberate choice, not just
convenience: ``write_micro_batch`` processes each partition inside
``batch_df.rdd.mapPartitions``, which Spark always executes in a separate
Python worker process (even under the single-core ``local[1]`` test
session) that re-imports this module fresh -- a ``monkeypatch`` applied to
a module attribute in the test process has no effect there, and neither
would it affect a *default* parameter value, since defaults are bound once
at function-definition time. Passing paths and stub functions explicitly as
arguments is what actually works across that process boundary, and it's
what production code does too (``write_micro_batch``'s own defaults are
just the real config paths). The real end-to-end wiring (multiple
partitions, real inserts) is covered instead by the integration test in
test_db.py, against a real database.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone

from src.streaming_pipeline import sink

_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _valid_row(event_id: str) -> dict:
    return {
        "event_id": event_id, "event_time": _NOW, "user_id": 1, "session_id": "s1",
        "event_type": "purchase", "product_id": "P001", "product_name": "Widget",
        "category": "Electronics", "price": 9.99, "quantity": 1, "total_amount": 9.99,
        "processing_time": _NOW, "is_valid": True, "validation_reason": None,
    }


def _invalid_row(event_id: str, reason: str) -> dict:
    row = _valid_row(event_id)
    row.update(is_valid=False, validation_reason=reason, total_amount=None)
    return row


def test_process_partition_inserts_only_valid_rows_and_returns_counts(tmp_path):
    inserted_records = []

    def fake_insert(records):
        inserted_records.extend(records)
        return len(records)

    rows = [_valid_row("v1"), _valid_row("v2"), _invalid_row("i1", "invalid_price")]
    received, inserted, rejected = sink.process_partition(
        rows, batch_id=7, partition_id=0, insert_fn=fake_insert, rejected_dir=tmp_path / "rejected",
    )

    assert (received, inserted, rejected) == (3, 2, 1)
    assert {r["event_id"] for r in inserted_records} == {"v1", "v2"}
    assert set(inserted_records[0].keys()) == set(sink.INSERT_COLUMNS)

    rejected_path = tmp_path / "rejected" / "rejected_batch_7_part_0.csv"
    assert rejected_path.exists()
    with rejected_path.open() as f:
        rejected_rows = list(csv.DictReader(f))
    assert len(rejected_rows) == 1
    assert rejected_rows[0]["event_id"] == "i1"
    assert rejected_rows[0]["validation_reason"] == "invalid_price"


def test_process_partition_skips_insert_when_no_valid_rows(tmp_path):
    calls = []

    def fake_insert(records):
        calls.append(records)
        return 0

    rows = [_invalid_row("i1", "invalid_price"), _invalid_row("i2", "missing_product_id")]
    received, inserted, rejected = sink.process_partition(
        rows, batch_id=2, partition_id=1, insert_fn=fake_insert, rejected_dir=tmp_path / "rejected",
    )

    assert (received, inserted, rejected) == (2, 0, 2)
    assert calls == []
    assert (tmp_path / "rejected" / "rejected_batch_2_part_1.csv").exists()


def test_process_partition_all_valid_writes_no_rejected_file(tmp_path):
    rows = [_valid_row("v1"), _valid_row("v2")]
    received, inserted, rejected = sink.process_partition(
        rows, batch_id=3, partition_id=0,
        insert_fn=lambda records: len(records), rejected_dir=tmp_path / "rejected",
    )

    assert (received, inserted, rejected) == (2, 2, 0)
    assert not (tmp_path / "rejected").exists()


def test_process_partition_with_no_rows_in_partition(tmp_path):
    calls = []

    received, inserted, rejected = sink.process_partition(
        [], batch_id=4, partition_id=2,
        insert_fn=lambda records: calls.append(records) or len(records), rejected_dir=tmp_path / "rejected",
    )

    assert (received, inserted, rejected) == (0, 0, 0)
    assert calls == []
    assert not (tmp_path / "rejected").exists()


def test_write_micro_batch_skips_empty_batches(spark, tmp_path):
    empty_df = spark.createDataFrame([], schema="event_id string, is_valid boolean")
    sink.write_micro_batch(
        empty_df, batch_id=0,
        rejected_dir=tmp_path / "rejected", row_counts_path=tmp_path / "batch_row_counts.csv",
    )

    assert not (tmp_path / "batch_row_counts.csv").exists()
