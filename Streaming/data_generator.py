"""Simulate an e-commerce clickstream by periodically writing CSV event files.

Each run drops a batch of fake "view" / "add_to_cart" / "purchase" events into
data/incoming/ as a new CSV file, on a fixed interval, so that a file-watching
Spark Structured Streaming job (spark_streaming_to_postgres.py) has a steady
stream of new files to pick up.
"""
from __future__ import annotations

import argparse
import random
import time

from src.streaming_pipeline.config import (
    DEFAULT_EVENTS_PER_FILE,
    DEFAULT_FILE_INTERVAL_SECONDS,
)
from src.streaming_pipeline.events import generate_batch
from src.streaming_pipeline.generator_io import write_events_csv
from src.streaming_pipeline.logger_config import configure_logger

logger = configure_logger("data_generator")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--num-files", type=int, default=None,
        help="Number of CSV files to generate before stopping (default: run until interrupted)",
    )
    parser.add_argument(
        "--events-per-file", type=int, default=DEFAULT_EVENTS_PER_FILE,
        help=f"Events per CSV file (default: {DEFAULT_EVENTS_PER_FILE})",
    )
    parser.add_argument(
        "--interval", type=float, default=DEFAULT_FILE_INTERVAL_SECONDS,
        help=f"Seconds to wait between files (default: {DEFAULT_FILE_INTERVAL_SECONDS})",
    )
    parser.add_argument("--seed", type=int, default=None, help="Random seed, for reproducible demo runs")
    return parser.parse_args()


def run(num_files: int | None, events_per_file: int, interval: float, rng: random.Random) -> int:
    """Generate CSV files until ``num_files`` is reached (or forever if None).

    Returns the number of files successfully written.
    """
    files_written = 0
    while num_files is None or files_written < num_files:
        try:
            batch = generate_batch(events_per_file, rng)
            write_events_csv(batch)
            files_written += 1
        except Exception:
            logger.exception("Failed to generate/write a batch; continuing to the next interval")

        is_last_file = num_files is not None and files_written >= num_files
        if not is_last_file:
            time.sleep(interval)

    return files_written


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    logger.info(
        "Starting data generator (num_files=%s, events_per_file=%d, interval=%.1fs, seed=%s)",
        args.num_files, args.events_per_file, args.interval, args.seed,
    )
    try:
        files_written = run(args.num_files, args.events_per_file, args.interval, rng)
        logger.info("Data generator finished after writing %d file(s)", files_written)
    except KeyboardInterrupt:
        logger.info("Data generator stopped by user (KeyboardInterrupt)")


if __name__ == "__main__":
    main()
