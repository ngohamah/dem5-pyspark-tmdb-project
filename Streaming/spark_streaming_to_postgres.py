"""Spark Structured Streaming job: watch data/incoming/ for new CSV event
files, clean/type-convert each micro-batch, and write valid rows to
PostgreSQL. Invalid rows are logged and saved under data/rejected/ instead
of being silently dropped.
"""
from __future__ import annotations

import argparse

from src.streaming_pipeline.config import (
    CHECKPOINT_DIR,
    INCOMING_DIR,
    MAX_FILES_PER_TRIGGER,
    STREAM_TRIGGER_INTERVAL,
)
from src.streaming_pipeline.logger_config import configure_logger
from src.streaming_pipeline.metrics import MetricsListener
from src.streaming_pipeline.schema import RAW_EVENT_SCHEMA
from src.streaming_pipeline.sink import write_micro_batch
from src.streaming_pipeline.spark_utils import get_spark_session
from src.streaming_pipeline.transform import clean_events

logger = configure_logger("spark_streaming_to_postgres")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--once", action="store_true",
        help="Process every file currently in data/incoming/ then stop, instead of running continuously",
    )
    parser.add_argument(
        "--duration", type=float, default=None,
        help="Stop the continuous stream after this many seconds (default: run until interrupted)",
    )
    return parser.parse_args()


def build_writer(spark):
    """Build the (not-yet-started) streaming writer: read -> clean -> sink."""
    raw_stream = (
        spark.readStream.schema(RAW_EVENT_SCHEMA)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .csv(str(INCOMING_DIR))
    )
    cleaned_stream = clean_events(raw_stream)
    return (
        cleaned_stream.writeStream.foreachBatch(write_micro_batch)
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .outputMode("append")
    )


def main() -> None:
    args = parse_args()
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session()
    spark.streams.addListener(MetricsListener())

    writer = build_writer(spark)
    trigger_kwargs = {"availableNow": True} if args.once else {"processingTime": STREAM_TRIGGER_INTERVAL}

    try:
        query = writer.trigger(**trigger_kwargs).start()
        logger.info("Streaming query started (once=%s, watching %s)", args.once, INCOMING_DIR)
        if args.once:
            query.awaitTermination()
        elif query.awaitTermination(timeout=args.duration) is False:
            logger.info("Reached --duration=%.1fs; stopping the streaming query", args.duration)
            query.stop()
    except KeyboardInterrupt:
        logger.info("Streaming query stopped by user (KeyboardInterrupt)")
    except Exception:
        logger.exception("Streaming query failed")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
