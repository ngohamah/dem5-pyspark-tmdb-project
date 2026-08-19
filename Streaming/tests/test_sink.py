"""Tests for src/streaming_pipeline/sink.py, with Postgres mocked out.

Verifies the routing logic (valid rows -> insert_events, invalid rows ->
rejected CSV, every batch -> row-count log) without touching a real database.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.streaming_pipeline import sink

_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)

_BATCH_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("user_id", IntegerType()),
    StructField("session_id", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("total_amount", DoubleType()),
    StructField("processing_time", TimestampType()),
    StructField("is_valid", BooleanType()),
    StructField("validation_reason", StringType()),
])


def _make_batch_df(spark, rows):
    columns = [field.name for field in _BATCH_SCHEMA.fields]
    return spark.createDataFrame([tuple(row[c] for c in columns) for row in rows], schema=_BATCH_SCHEMA)


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


def test_write_micro_batch_inserts_only_valid_rows_and_records_counts(spark, tmp_path, monkeypatch):
    inserted_records = []
    monkeypatch.setattr(sink, "insert_events", lambda records: inserted_records.extend(records) or len(records))
    monkeypatch.setattr(sink, "REJECTED_DIR", tmp_path / "rejected")
    monkeypatch.setattr(sink, "BATCH_ROW_COUNTS_PATH", tmp_path / "batch_row_counts.csv")

    batch_df = _make_batch_df(spark, [_valid_row("v1"), _valid_row("v2"), _invalid_row("i1", "invalid_price")])
    sink.write_micro_batch(batch_df, batch_id=7)

    assert {r["event_id"] for r in inserted_records} == {"v1", "v2"}

    rejected_path = tmp_path / "rejected" / "rejected_batch_7.csv"
    assert rejected_path.exists()
    with rejected_path.open() as f:
        rejected_rows = list(csv.DictReader(f))
    assert len(rejected_rows) == 1
    assert rejected_rows[0]["event_id"] == "i1"

    with (tmp_path / "batch_row_counts.csv").open() as f:
        counts = list(csv.DictReader(f))
    assert counts == [{"batch_id": "7", "received": "3", "inserted": "2", "rejected": "1"}]


def test_write_micro_batch_skips_empty_batches(spark, tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(sink, "insert_events", lambda records: calls.append(records) or 0)
    monkeypatch.setattr(sink, "BATCH_ROW_COUNTS_PATH", tmp_path / "batch_row_counts.csv")

    empty_df = _make_batch_df(spark, [])
    sink.write_micro_batch(empty_df, batch_id=0)

    assert calls == []
    assert not (tmp_path / "batch_row_counts.csv").exists()


def test_write_micro_batch_all_valid_writes_no_rejected_file(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(sink, "insert_events", lambda records: len(records))
    monkeypatch.setattr(sink, "REJECTED_DIR", tmp_path / "rejected")
    monkeypatch.setattr(sink, "BATCH_ROW_COUNTS_PATH", tmp_path / "batch_row_counts.csv")

    batch_df = _make_batch_df(spark, [_valid_row("v1")])
    sink.write_micro_batch(batch_df, batch_id=1)

    assert not (tmp_path / "rejected").exists()
