"""Tests for the fake event generator in src/streaming_pipeline/events.py."""
from __future__ import annotations

import random

from src.streaming_pipeline.config import CSV_COLUMNS, EVENT_TYPES
from src.streaming_pipeline.events import generate_batch, generate_event


def test_generate_event_has_all_expected_columns():
    event = generate_event(random.Random(1))
    assert set(event.keys()) == set(CSV_COLUMNS)


def test_generate_event_is_reproducible_with_a_seeded_rng():
    # event_id/session_id (uuid4) and event_time (wall clock) are intentionally
    # not derived from the rng, so compare only the fields the rng determines.
    rng_derived_fields = ("user_id", "event_type", "product_id", "product_name", "category", "price", "quantity")
    event_a = generate_event(random.Random(42))
    event_b = generate_event(random.Random(42))
    assert {k: event_a[k] for k in rng_derived_fields} == {k: event_b[k] for k in rng_derived_fields}


def test_generate_batch_returns_the_requested_count():
    batch = generate_batch(25, random.Random(1))
    assert len(batch) == 25


def test_generate_batch_produces_only_known_event_types_or_the_injected_unknown():
    batch = generate_batch(200, random.Random(7))
    seen_types = {event["event_type"] for event in batch}
    assert seen_types <= set(EVENT_TYPES) | {"unknown"}


def test_force_corruption_negative_price():
    event = generate_event(random.Random(1), force_corruption="negative_price")
    assert event["price"] < 0


def test_force_corruption_missing_product_id():
    event = generate_event(random.Random(1), force_corruption="missing_product_id")
    assert event["product_id"] == ""


def test_force_corruption_unknown_event_type():
    event = generate_event(random.Random(1), force_corruption="unknown_event_type")
    assert event["event_type"] == "unknown"


def test_force_corruption_bad_timestamp():
    event = generate_event(random.Random(1), force_corruption="bad_timestamp")
    assert event["event_time"] == "not-a-timestamp"


def test_force_corruption_negative_quantity():
    event = generate_event(random.Random(1), force_corruption="negative_quantity")
    assert event["quantity"] < 0


def test_malformed_event_rate_produces_some_corrupted_events_over_a_large_batch():
    batch = generate_batch(2000, random.Random(3))
    valid_types = set(EVENT_TYPES)
    corrupted = [
        e for e in batch
        if e["price"] < 0 or not e["product_id"] or e["event_type"] not in valid_types
        or e["event_time"] == "not-a-timestamp" or e["quantity"] < 0
    ]
    assert 0 < len(corrupted) < len(batch)
