"""Tests for the atomic CSV writer in src/streaming_pipeline/generator_io.py."""
from __future__ import annotations

import csv
import random

from src.streaming_pipeline.events import generate_batch
from src.streaming_pipeline.generator_io import write_events_csv


def test_write_events_csv_creates_one_file_with_a_header_and_all_rows(tmp_path):
    events = generate_batch(10, random.Random(1))
    path = write_events_csv(events, directory=tmp_path)

    assert path.exists()
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 10
    assert rows[0].keys() == events[0].keys()


def test_write_events_csv_leaves_no_temp_files_behind(tmp_path):
    write_events_csv(generate_batch(5, random.Random(2)), directory=tmp_path)
    leftover_tmp_files = list(tmp_path.glob(".tmp_*"))
    assert leftover_tmp_files == []


def test_write_events_csv_skips_empty_batches(tmp_path):
    result = write_events_csv([], directory=tmp_path)
    assert result is None
    assert list(tmp_path.iterdir()) == []


def test_write_events_csv_writes_distinct_files_for_successive_batches(tmp_path):
    path_a = write_events_csv(generate_batch(3, random.Random(1)), directory=tmp_path)
    path_b = write_events_csv(generate_batch(3, random.Random(2)), directory=tmp_path)
    assert path_a != path_b
    assert len(list(tmp_path.glob("events_*.csv"))) == 2
