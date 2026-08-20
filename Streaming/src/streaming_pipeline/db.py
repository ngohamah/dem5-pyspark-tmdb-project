"""PostgreSQL access for the streaming pipeline.

Each micro-batch opens exactly one connection, inserts every valid row from
that batch in a single statement, then closes the connection -- rather than
opening a connection per row -- so the sink can't overwhelm Postgres with
connection churn as batches arrive continuously.
"""
from __future__ import annotations

from typing import Any

import psycopg2
from psycopg2.extras import execute_values

from .config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_TABLE,
    POSTGRES_USER,
)
from .logger_config import configure_logger

logger = configure_logger(__name__)

INSERT_COLUMNS = (
    "event_id", "event_time", "user_id", "session_id", "event_type",
    "product_id", "product_name", "category", "price", "quantity",
    "total_amount", "processing_time",
)


def get_connection():
    """Open a new connection to the events database.

    Sessions are pinned to UTC: Spark's session timezone is also UTC
    (spark_utils.get_spark_session), so naive datetimes arriving from a
    micro-batch are already UTC wall-clock values -- pinning the session
    here means Postgres interprets them the same way rather than assuming
    whatever the host machine's local timezone happens to be.
    """
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    return conn


def _row_to_tuple(row: dict[str, Any]) -> tuple:
    return tuple(row[column] for column in INSERT_COLUMNS)


def insert_events(rows: list[dict[str, Any]], conn=None) -> int:
    """Insert valid event rows, skipping any ``event_id`` already present.

    Returns the number of rows actually inserted (duplicates are silently
    skipped via ON CONFLICT, so replaying a micro-batch after a restart from
    checkpoint is safe). Opens and closes its own connection unless one is
    passed in, e.g. by a test that wants to inspect the same transaction.
    """
    if not rows:
        return 0

    owns_connection = conn is None
    try:
        conn = conn or get_connection()
        with conn.cursor() as cur:
            query = f"""
                INSERT INTO {POSTGRES_TABLE} ({", ".join(INSERT_COLUMNS)})
                VALUES %s
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
            """
            # execute_values pages a large `rows` list into multiple internal
            # INSERT statements (page_size=100 by default) -- cur.rowcount
            # afterward would only reflect the last page, not the total.
            # fetch=True instead concatenates every page's RETURNING rows,
            # so len() here is accurate regardless of how many pages ran.
            inserted_ids = execute_values(
                cur, query, [_row_to_tuple(row) for row in rows], fetch=True,
            )
            inserted = len(inserted_ids)
        conn.commit()
        logger.info("Inserted %d/%d row(s) into %s", inserted, len(rows), POSTGRES_TABLE)
        return inserted
    except Exception:
        if conn is not None:
            conn.rollback()
        logger.exception("Failed to insert %d row(s) into %s; transaction rolled back", len(rows), POSTGRES_TABLE)
        raise
    finally:
        if owns_connection and conn is not None:
            conn.close()
