"""Tests for src/streaming_pipeline/transform.py: casting + row validation."""
from __future__ import annotations

from src.streaming_pipeline.config import CORRUPT_RECORD_COLUMN, CSV_COLUMNS
from src.streaming_pipeline.schema import RAW_EVENT_SCHEMA
from src.streaming_pipeline.transform import clean_events, reduce_reasons_to_counts

_VALID_ROW = {
    "event_id": "11111111-1111-1111-1111-111111111111",
    "event_time": "2026-08-19T15:05:46.099484+00:00",
    "user_id": "42",
    "session_id": "session-1",
    "event_type": "purchase",
    "product_id": "P001",
    "product_name": "Wireless Mouse",
    "category": "Electronics",
    "price": "19.99",
    "quantity": "2",
}


def _make_row(**overrides) -> dict:
    row = {**_VALID_ROW, **overrides}
    row[CORRUPT_RECORD_COLUMN] = None
    return row


def _make_df(spark, rows: list[dict]):
    ordered_columns = CSV_COLUMNS + [CORRUPT_RECORD_COLUMN]
    data = [tuple(row.get(c) for c in ordered_columns) for row in rows]
    return spark.createDataFrame(data, schema=RAW_EVENT_SCHEMA)


def _row_by_event_id(cleaned_df, event_id: str) -> dict:
    rows = cleaned_df.filter(cleaned_df.event_id == event_id).collect()
    assert len(rows) == 1
    return rows[0].asDict()


def test_valid_row_is_flagged_valid_with_correct_types(spark):
    df = _make_df(spark, [_make_row()])
    row = _row_by_event_id(clean_events(df), _VALID_ROW["event_id"])
    assert row["is_valid"] is True
    assert row["validation_reason"] is None
    assert row["user_id"] == 42
    assert row["price"] == 19.99
    assert row["quantity"] == 2
    assert row["total_amount"] == 39.98


def test_negative_price_is_invalid(spark):
    df = _make_df(spark, [_make_row(price="-5.00")])
    row = _row_by_event_id(clean_events(df), _VALID_ROW["event_id"])
    assert row["is_valid"] is False
    assert row["validation_reason"] == "invalid_price"
    assert row["total_amount"] is None


def test_missing_product_id_is_invalid(spark):
    df = _make_df(spark, [_make_row(product_id="")])
    row = _row_by_event_id(clean_events(df), _VALID_ROW["event_id"])
    assert row["validation_reason"] == "missing_product_id"


def test_unknown_event_type_is_invalid(spark):
    df = _make_df(spark, [_make_row(event_type="refund")])
    row = _row_by_event_id(clean_events(df), _VALID_ROW["event_id"])
    assert row["validation_reason"] == "invalid_event_type"


def test_unparseable_timestamp_is_invalid(spark):
    df = _make_df(spark, [_make_row(event_time="not-a-timestamp")])
    row = _row_by_event_id(clean_events(df), _VALID_ROW["event_id"])
    assert row["validation_reason"] == "invalid_event_time"
    assert row["event_time"] is None


def test_negative_quantity_is_invalid(spark):
    df = _make_df(spark, [_make_row(quantity="-1")])
    row = _row_by_event_id(clean_events(df), _VALID_ROW["event_id"])
    assert row["validation_reason"] == "invalid_quantity"


def test_non_numeric_user_id_is_invalid(spark):
    df = _make_df(spark, [_make_row(user_id="not-a-number")])
    row = _row_by_event_id(clean_events(df), _VALID_ROW["event_id"])
    assert row["validation_reason"] == "invalid_user_id"


def test_missing_event_id_is_invalid(spark):
    df = _make_df(spark, [_make_row(event_id="")])
    rows = clean_events(df).collect()
    assert len(rows) == 1
    assert rows[0]["validation_reason"] == "missing_event_id"


def test_first_matching_rule_wins_when_multiple_fields_are_invalid(spark):
    # Both product_id and price are broken; missing_product_id is checked first.
    df = _make_df(spark, [_make_row(product_id="", price="-1")])
    row = _row_by_event_id(clean_events(df), _VALID_ROW["event_id"])
    assert row["validation_reason"] == "missing_product_id"


def test_reduce_reasons_to_counts_tallies_each_reason():
    counts = reduce_reasons_to_counts(["invalid_price", "invalid_price", "missing_product_id"])
    assert counts == {"invalid_price": 2, "missing_product_id": 1}
