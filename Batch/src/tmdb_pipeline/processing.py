"""Data cleaning and transformation for TMDB movie data, in PySpark.

JSON-like fields (structs/arrays returned by the TMDB API) are flattened
using Spark's native higher-order array functions (``transform``,
``filter``) rather than row-by-row Python UDFs -- this keeps the cleaning
stage a declarative, functional chain of column expressions that Spark's
optimizer can plan as a whole, instead of forcing per-row Python calls.
"""
from __future__ import annotations

import json
from collections.abc import Callable
from functools import reduce
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import (
    FINAL_COLUMNS,
    IRRELEVANT_COLUMNS,
    JSON_ARRAY_COLUMNS,
    JSON_STRUCT_COLUMNS,
    MIN_NON_NULL_COLUMNS,
    RAW_SPARK_INPUT_PATH,
)
from .logger_config import configure_logger

logger = configure_logger(__name__)


def pipe(value: Any, *functions: Callable[[Any], Any]) -> Any:
    """Thread ``value`` through a sequence of single-argument functions."""
    return reduce(lambda acc, fn: fn(acc), functions, value)


def write_payloads_as_jsonl(payloads: list[dict[str, Any]], path: Path = RAW_SPARK_INPUT_PATH) -> Path:
    """Persist raw payloads as newline-delimited JSON for Spark to ingest."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for payload in payloads:
            handle.write(json.dumps(payload) + "\n")
    logger.info("Wrote %d raw movie records to %s for Spark ingestion", len(payloads), path)
    return path


def load_raw_movies(spark: SparkSession, payloads: list[dict[str, Any]]) -> DataFrame:
    """Write payloads to disk and load them as a Spark DataFrame with inferred schema."""
    path = write_payloads_as_jsonl(payloads)
    return spark.read.json(str(path))


def drop_irrelevant_columns(df: DataFrame) -> DataFrame:
    present = [c for c in IRRELEVANT_COLUMNS if c in df.columns]
    if present:
        logger.info("Dropping irrelevant columns: %s", present)
    return df.drop(*present)


def flatten_struct_columns(df: DataFrame) -> DataFrame:
    """Pull the ``name`` field out of single-object JSON columns (e.g. belongs_to_collection)."""
    for column in JSON_STRUCT_COLUMNS:
        if column in df.columns:
            df = df.withColumn(column, F.col(f"{column}.name"))
    return df


def flatten_array_columns(df: DataFrame) -> DataFrame:
    """Join the ``name`` field of array-of-object JSON columns with '|'.

    A genuinely missing array (the key was absent from the payload) stays
    null rather than becoming ``""`` -- ``concat_ws`` on a null array would
    otherwise silently produce an empty string, which is not the same thing
    and would corrupt the "at least 10 non-null columns" check downstream.
    """
    for column in JSON_ARRAY_COLUMNS:
        if column in df.columns:
            original = F.col(column)
            names = F.transform(original, lambda item: item["name"])
            df = df.withColumn(column, F.when(original.isNotNull(), F.concat_ws("|", names)))
    return df


def extract_cast_and_crew(df: DataFrame) -> DataFrame:
    """Derive cast/cast_size/director/crew_size from the nested `credits` field."""
    if "credits" not in df.columns:
        return df

    has_credits = F.col("credits").isNotNull()
    cast_array = F.col("credits.cast")
    crew_array = F.col("credits.crew")

    cast_names = F.transform(cast_array, lambda item: item["name"])
    directors = F.filter(crew_array, lambda item: item["job"] == F.lit("Director"))
    director_names = F.transform(directors, lambda item: item["name"])

    return (
        df.withColumn("cast", F.when(has_credits, F.concat_ws("|", cast_names)))
        .withColumn("cast_size", F.when(has_credits, F.size(F.coalesce(cast_array, F.array()))))
        .withColumn("director", F.when(has_credits, F.element_at(director_names, 1)))
        .withColumn("crew_size", F.when(has_credits, F.size(F.coalesce(crew_array, F.array()))))
        .drop("credits")
    )


def cast_column_types(df: DataFrame) -> DataFrame:
    """Coerce raw string/mixed columns to their proper numeric/date types."""
    numeric_casts = {
        "budget": "double",
        "id": "long",
        "popularity": "double",
        "revenue": "double",
        "runtime": "double",
        "vote_count": "long",
        "vote_average": "double",
    }
    for column, spark_type in numeric_casts.items():
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast(spark_type))
    if "release_date" in df.columns:
        df = df.withColumn("release_date", F.to_date("release_date"))
    return df


def null_out_unrealistic_values(df: DataFrame) -> DataFrame:
    """Treat 0 budget/revenue/runtime and placeholder text as missing data."""
    for column in ("budget", "revenue", "runtime"):
        if column in df.columns:
            df = df.withColumn(column, F.when(F.col(column) == 0, None).otherwise(F.col(column)))
    for column in ("overview", "tagline"):
        if column in df.columns:
            df = df.withColumn(
                column,
                F.when(F.col(column).isin("No Data", ""), None).otherwise(F.col(column)),
            )
    return df


def derive_financial_metrics(df: DataFrame) -> DataFrame:
    """Compute budget/revenue in million-USD, plus profit and ROI."""
    df = df.withColumn("budget_musd", F.col("budget") / F.lit(1_000_000))
    df = df.withColumn("revenue_musd", F.col("revenue") / F.lit(1_000_000))
    df = df.withColumn("profit_musd", F.col("revenue_musd") - F.col("budget_musd"))
    df = df.withColumn(
        "roi",
        F.when((F.col("budget_musd").isNull()) | (F.col("budget_musd") == 0), None)
        .otherwise(F.col("revenue_musd") / F.col("budget_musd")),
    )
    return df


def drop_duplicates_and_unknowns(df: DataFrame) -> DataFrame:
    before = df.count()
    df = df.dropDuplicates(["id"]).dropna(subset=["id", "title"])
    after = df.count()
    if before != after:
        logger.info("Dropped %d duplicate/unknown-id-or-title rows", before - after)
    return df


def drop_sparse_rows(df: DataFrame, min_non_null: int = MIN_NON_NULL_COLUMNS) -> DataFrame:
    """Keep only rows with at least ``min_non_null`` non-null columns."""
    non_null_count = reduce(
        lambda acc, c: acc + F.when(F.col(c).isNotNull(), 1).otherwise(0),
        df.columns,
        F.lit(0),
    )
    before = df.count()
    df = df.withColumn("_non_null_count", non_null_count).filter(
        F.col("_non_null_count") >= min_non_null
    ).drop("_non_null_count")
    after = df.count()
    if before != after:
        logger.info("Dropped %d sparse rows with fewer than %d populated columns", before - after, min_non_null)
    return df


def keep_only_released(df: DataFrame) -> DataFrame:
    if "status" not in df.columns:
        return df
    before = df.count()
    df = df.filter(F.col("status") == "Released").drop("status")
    after = df.count()
    if before != after:
        logger.info("Dropped %d non-Released rows", before - after)
    return df


def reorder_and_select(df: DataFrame) -> DataFrame:
    for column in FINAL_COLUMNS:
        if column not in df.columns:
            df = df.withColumn(column, F.lit(None))
    return df.select(*FINAL_COLUMNS)


def clean_movie_data(spark: SparkSession, payloads: list[dict[str, Any]]) -> DataFrame:
    """Run the full cleaning pipeline over raw TMDB payloads."""
    logger.info("Starting data cleaning for %d records", len(payloads))
    try:
        cleaned = pipe(
            load_raw_movies(spark, payloads),
            drop_irrelevant_columns,
            flatten_struct_columns,
            flatten_array_columns,
            extract_cast_and_crew,
            cast_column_types,
            null_out_unrealistic_values,
            derive_financial_metrics,
            drop_duplicates_and_unknowns,
            drop_sparse_rows,
            keep_only_released,
            reorder_and_select,
        )
        row_count = cleaned.count()
        logger.info("Cleaning completed with %d rows", row_count)
        return cleaned
    except Exception:
        logger.exception("Cleaning failed")
        raise


def write_single_csv(df: DataFrame, path: Path) -> Path:
    """Write a small Spark DataFrame out as one plain CSV file (not a part-file directory)."""
    tmp_dir = path.parent / f"_{path.stem}_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(tmp_dir))

    part_file = next(tmp_dir.glob("part-*.csv"))
    path.parent.mkdir(parents=True, exist_ok=True)
    part_file.replace(path)

    for leftover in tmp_dir.iterdir():
        leftover.unlink()
    tmp_dir.rmdir()

    logger.info("Wrote cleaned dataset to %s", path)
    return path
