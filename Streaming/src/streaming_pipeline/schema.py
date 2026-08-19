"""Spark schema for the raw event CSV files.

Every field is read as a string. Type conversion (int, decimal, timestamp)
happens explicitly in transform.py instead of being inferred by the CSV
reader, so a malformed value (e.g. a corrupted "price") lands as a string
we can inspect and reject, rather than silently becoming a null before we
ever see it.
"""
from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

from .config import CORRUPT_RECORD_COLUMN, CSV_COLUMNS

RAW_EVENT_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in CSV_COLUMNS]
    + [StructField(CORRUPT_RECORD_COLUMN, StringType(), True)]
)
