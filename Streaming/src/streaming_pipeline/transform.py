"""Data cleaning and type conversion for the raw event stream.

``clean_events`` never drops rows itself -- it casts every field to its
target type and flags each row with ``is_valid`` / ``validation_reason``.
The caller (spark_streaming_to_postgres.py) decides what to do with valid
vs. invalid rows, which keeps this function a single, easily-testable
Spark transformation with no I/O.
"""
from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .config import CORRUPT_RECORD_COLUMN, EVENT_TYPES

_EVENT_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"


def _blank_to_null(column: str) -> Column:
    """Treat an empty/whitespace-only string the same as a missing value."""
    return F.when(F.trim(F.col(column)) == "", None).otherwise(F.trim(F.col(column)))


def _cast_types(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("event_id", _blank_to_null("event_id"))
        .withColumn("event_time", F.to_timestamp("event_time", _EVENT_TIME_FORMAT))
        .withColumn("user_id", F.col("user_id").cast("int"))
        .withColumn("session_id", _blank_to_null("session_id"))
        .withColumn("event_type", _blank_to_null("event_type"))
        .withColumn("product_id", _blank_to_null("product_id"))
        .withColumn("product_name", _blank_to_null("product_name"))
        .withColumn("category", _blank_to_null("category"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("int"))
    )


def _add_validation_reason(df: DataFrame) -> DataFrame:
    """Flag each row with the first validation rule it fails, if any.

    Rules are checked in order via ``coalesce``, which returns its first
    non-null argument -- so a row failing rules 1 and 3 is reported as
    failing rule 1, giving one unambiguous reason per row instead of a set.
    """
    rules: list[tuple[str, Column]] = [
        ("corrupt_csv_row", F.col(CORRUPT_RECORD_COLUMN).isNotNull()),
        ("missing_event_id", F.col("event_id").isNull()),
        ("invalid_event_time", F.col("event_time").isNull()),
        ("invalid_user_id", F.col("user_id").isNull()),
        ("invalid_event_type", ~F.col("event_type").isin(*EVENT_TYPES)),
        ("missing_product_id", F.col("product_id").isNull()),
        ("invalid_price", F.col("price").isNull() | (F.col("price") <= 0)),
        ("invalid_quantity", F.col("quantity").isNull() | (F.col("quantity") <= 0)),
    ]
    validation_reason = F.coalesce(*(F.when(cond, F.lit(reason)) for reason, cond in rules))
    return df.withColumn("validation_reason", validation_reason).withColumn(
        "is_valid", F.col("validation_reason").isNull()
    )


def clean_events(df: DataFrame) -> DataFrame:
    """Cast raw string columns to their target types and flag row validity.

    Adds ``total_amount`` (price * quantity) and ``processing_time`` (when
    Spark processed the row) to every row, valid or not, so a downstream
    consumer can inspect rejected rows with full context.
    """
    typed = _cast_types(df)
    flagged = _add_validation_reason(typed)
    return flagged.withColumn(
        "total_amount",
        F.when(F.col("is_valid"), F.round(F.col("price") * F.col("quantity"), 2)),
    ).withColumn("processing_time", F.current_timestamp())


def reduce_reasons_to_counts(reasons: list[str]) -> dict[str, int]:
    """Tally validation reasons for a log line -- e.g. {'invalid_price': 3}."""
    return reduce(lambda counts, reason: {**counts, reason: counts.get(reason, 0) + 1}, reasons, {})
