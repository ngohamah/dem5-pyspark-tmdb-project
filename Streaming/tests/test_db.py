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

import getpass
import uuid
from datetime import datetime, timezone

import psycopg2
import pytest

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
