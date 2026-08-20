"""Integration tests for src/streaming_pipeline/db.py against a real Postgres.

These run against the local `streaming_events` database set up by
postgres_setup.sql. If that database isn't reachable (e.g. CI without a
Postgres service), the whole module is skipped rather than failed, since
these tests exercise real I/O, not pure logic.

insert_events() commits internally (each micro-batch is its own
transaction in production), so a plain rollback can't clean up after a
test -- instead, each test uses a fresh random event_id and an explicit
teardown DELETE.
"""
from __future__ import annotations

import csv
import getpass
import uuid
from datetime import datetime, timezone

import psycopg2
import pytest

from src.streaming_pipeline import sink
from src.streaming_pipeline.config import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT
from src.streaming_pipeline.db import get_connection, insert_events

try:
    _probe = get_connection()
    _probe.close()
except psycopg2.OperationalError as exc:
    pytest.skip(f"Postgres not reachable, skipping db integration tests: {exc}", allow_module_level=True)


def _superuser_connection():
    """A connection with delete rights, for test cleanup only.

    streaming_app intentionally has only SELECT/INSERT on `events` (see
    postgres_setup.sql) -- the ingestion job never needs to delete rows, so
    it doesn't get the privilege. Test teardown needs it, so it connects as
    the local OS user instead, which postgres_setup.sql granted superuser
    rights via trust/peer auth (see local Postgres setup).
    """
    return psycopg2.connect(host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB, user=getpass.getuser())


def _sample_row(event_id: str, **overrides) -> dict:
    row = {
        "event_id": event_id,
        "event_time": datetime.now(timezone.utc),
        "user_id": 1,
        "session_id": str(uuid.uuid4()),
        "event_type": "purchase",
        "product_id": "P001",
        "product_name": "Wireless Mouse",
        "category": "Electronics",
        "price": 19.99,
        "quantity": 2,
        "total_amount": 39.98,
        "processing_time": datetime.now(timezone.utc),
    }
    row.update(overrides)
    return row


@pytest.fixture
def event_id():
    """A fresh event_id per test, deleted from the real table on teardown."""
    generated_id = str(uuid.uuid4())
    yield generated_id
    conn = _superuser_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM events WHERE event_id = %s", (generated_id,))
        conn.commit()
    finally:
        conn.close()


def test_insert_events_returns_zero_for_an_empty_batch():
    assert insert_events([]) == 0


def test_insert_events_inserts_new_rows(event_id):
    row = _sample_row(event_id)
    inserted = insert_events([row])
    assert inserted == 1

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT product_name, quantity FROM events WHERE event_id = %s", (event_id,))
            assert cur.fetchone() == ("Wireless Mouse", 2)
    finally:
        conn.close()


def test_insert_events_skips_duplicate_event_ids(event_id):
    row = _sample_row(event_id)
    first = insert_events([row])
    second = insert_events([row])
    assert first == 1
    assert second == 0


def test_insert_events_raises_and_logs_when_the_database_is_unreachable(monkeypatch, caplog):
    import src.streaming_pipeline.db as db_module

    monkeypatch.setattr(db_module, "POSTGRES_HOST", "127.0.0.1")
    monkeypatch.setattr(db_module, "POSTGRES_PORT", 59999)  # nothing listens here

    with pytest.raises(psycopg2.OperationalError):
        insert_events([_sample_row(str(uuid.uuid4()))])

    assert "Failed to insert" in caplog.text


_BATCH_COLUMNS = [
    "event_id", "event_time", "user_id", "session_id", "event_type", "product_id",
    "product_name", "category", "price", "quantity", "total_amount", "processing_time",
    "is_valid", "validation_reason",
]


def test_write_micro_batch_inserts_across_partitions_against_a_real_database(spark, tmp_path):
    """End-to-end check of the mapPartitions-based sink (see sink.py): each
    partition inserts its own valid rows and writes its own rejected-rows
    file, independently, without the batch ever being collected onto the
    driver as a single pandas DataFrame.

    Needs a real Spark job (mapPartitions always runs in a separate Python
    worker process, even under the single-core test session), which is why
    this lives here rather than in test_sink.py -- Postgres can't be mocked
    across that process boundary, so this test exercises the real database
    instead, same as the rest of this module. For the same reason,
    ``rejected_dir``/``row_counts_path`` are passed to ``write_micro_batch``
    explicitly rather than monkeypatched: a worker process re-imports
    sink.py fresh, so it would never see a patched module attribute and
    this test would otherwise silently write into the tracked
    logs/batch_row_counts.csv and data/rejected/ used by real runs.
    """
    rejected_dir = tmp_path / "rejected"
    row_counts_path = tmp_path / "batch_row_counts.csv"

    ids = [str(uuid.uuid4()) for _ in range(4)]
    now = datetime.now(timezone.utc)
    rows = [
        (ids[0], now, 1, str(uuid.uuid4()), "purchase", "P001", "Wireless Mouse", "Electronics",
         19.99, 2, 39.98, now, True, None),
        (ids[1], now, 1, str(uuid.uuid4()), "purchase", "P001", "Wireless Mouse", "Electronics",
         19.99, 1, 19.99, now, True, None),
        (ids[2], now, 1, str(uuid.uuid4()), "bogus", "P001", "Wireless Mouse", "Electronics",
         19.99, 1, None, now, False, "invalid_event_type"),
        (ids[3], now, 1, str(uuid.uuid4()), "purchase", "P001", "Wireless Mouse", "Electronics",
         -5.0, 1, None, now, False, "invalid_price"),
    ]
    batch_df = spark.createDataFrame(rows, schema=_BATCH_COLUMNS).repartition(3)

    try:
        sink.write_micro_batch(batch_df, batch_id=999, rejected_dir=rejected_dir, row_counts_path=row_counts_path)

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT event_id FROM events WHERE event_id = ANY(%s::uuid[])", ([ids[0], ids[1]],))
                found_ids = {str(row[0]) for row in cur.fetchall()}
        finally:
            conn.close()
        assert found_ids == {ids[0], ids[1]}

        rejected_files = sorted(rejected_dir.glob("rejected_batch_999_part_*.csv"))
        assert rejected_files
        rejected_ids = set()
        for path in rejected_files:
            with path.open() as f:
                rejected_ids.update(row["event_id"] for row in csv.DictReader(f))
        assert rejected_ids == {ids[2], ids[3]}

        with row_counts_path.open() as f:
            counts = list(csv.DictReader(f))
        assert counts == [{"batch_id": "999", "received": "4", "inserted": "2", "rejected": "2"}]
    finally:
        conn = _superuser_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM events WHERE event_id = ANY(%s::uuid[])", (ids,))
            conn.commit()
        finally:
            conn.close()
