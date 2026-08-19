"""Pure functions for generating fake e-commerce clickstream events.

Every function here is deterministic given its inputs (including the
``random.Random`` instance it's handed), which keeps the generation logic
easy to unit test: pass a seeded ``Random`` and the output is reproducible.
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone
from typing import Any

from .config import (
    EVENT_TYPE_WEIGHTS,
    EVENT_TYPES,
    MALFORMED_EVENT_RATE,
    NUM_USERS,
    PRODUCT_CATALOG,
)

# Kinds of corruption injected into a small fraction of events, so the
# downstream Spark cleaning stage has real invalid rows to catch.
_CORRUPTIONS = (
    "negative_price",
    "missing_product_id",
    "unknown_event_type",
    "bad_timestamp",
    "negative_quantity",
)


def _pick_event_type(rng: random.Random) -> str:
    return rng.choices(EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS, k=1)[0]


def _pick_product(rng: random.Random) -> dict[str, Any]:
    return rng.choice(PRODUCT_CATALOG)


def _corrupt(event: dict[str, Any], kind: str) -> dict[str, Any]:
    """Return a copy of ``event`` with one field deliberately broken."""
    corrupted = dict(event)
    if kind == "negative_price":
        corrupted["price"] = -abs(corrupted["price"])
    elif kind == "missing_product_id":
        corrupted["product_id"] = ""
    elif kind == "unknown_event_type":
        corrupted["event_type"] = "unknown"
    elif kind == "bad_timestamp":
        corrupted["event_time"] = "not-a-timestamp"
    elif kind == "negative_quantity":
        corrupted["quantity"] = -1
    return corrupted


def generate_event(rng: random.Random | None = None, force_corruption: str | None = None) -> dict[str, Any]:
    """Build a single fake e-commerce event.

    ``force_corruption`` (one of ``_CORRUPTIONS``) bypasses the random rate
    and always injects that specific defect -- used by tests that need a
    deterministic invalid row rather than a random one.
    """
    rng = rng or random.Random()
    product = _pick_product(rng)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "user_id": rng.randint(1, NUM_USERS),
        "session_id": str(uuid.uuid4()),
        "event_type": _pick_event_type(rng),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "price": product["price"],
        "quantity": rng.randint(1, 3),
    }

    if force_corruption is not None:
        return _corrupt(event, force_corruption)
    if rng.random() < MALFORMED_EVENT_RATE:
        return _corrupt(event, rng.choice(_CORRUPTIONS))
    return event


def generate_batch(n: int, rng: random.Random | None = None) -> list[dict[str, Any]]:
    """Generate ``n`` independent fake events."""
    rng = rng or random.Random()
    return [generate_event(rng) for _ in range(n)]
