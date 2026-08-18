"""Fetch TMDB movie data once per run and cache it as a batch on disk.

Every movie is fetched with ``append_to_response=credits`` so cast/crew
come back in the same request as the movie details -- one HTTP call per
movie, no follow-up requests. Results are cached to ``RAW_PAYLOAD_PATH``;
subsequent runs reuse the cache instead of re-hitting the API, and the
session is always closed after the batch completes.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests

from .config import (
    MOVIE_IDS,
    RAW_PAYLOAD_PATH,
    SAMPLE_PAYLOAD_PATH,
    TMDB_API_KEY,
    TMDB_API_PARAMS,
    TMDB_API_URL,
)
from .logger_config import configure_logger

logger = configure_logger(__name__)


def load_cached_payloads(path: Path | None = None) -> list[dict[str, Any]]:
    """Load previously cached payloads from disk, if any."""
    payload_path = path or RAW_PAYLOAD_PATH
    if payload_path.exists():
        logger.info("Using cached payloads from %s", payload_path)
        return json.loads(payload_path.read_text(encoding="utf-8"))
    return []


def fetch_movie_payload(session: requests.Session, movie_id: int) -> dict[str, Any]:
    """Fetch one movie's details + credits from the TMDB API."""
    if not TMDB_API_KEY or TMDB_API_KEY == "YOUR_TMDB_API_KEY":
        logger.warning("TMDB API key is not configured; falling back to cached sample data")
        raise ValueError("TMDB API key is not configured")

    url = TMDB_API_URL.format(movie_id=movie_id)
    params = {**TMDB_API_PARAMS, "api_key": TMDB_API_KEY}

    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        logger.info("Fetched movie %s from TMDB", movie_id)
        return payload
    except requests.RequestException:
        logger.exception("Failed to fetch movie %s", movie_id)
        raise


def fetch_movie_batch(movie_ids: list[int] | None = None) -> list[dict[str, Any]]:
    """Fetch a batch of movie payloads, only pulling ids missing from the cache.

    Opens a single ``requests.Session`` for the whole batch and closes it
    once every id has been resolved, to avoid rate-limiting from repeated
    connection setup.
    """
    ids = movie_ids or MOVIE_IDS
    cached_payloads = load_cached_payloads(RAW_PAYLOAD_PATH)
    cached_by_id = {payload["id"]: payload for payload in cached_payloads}

    valid_ids: list[int] = []
    for movie_id in ids:
        if movie_id <= 0:
            logger.info("Skipping invalid movie id %s", movie_id)
            continue
        valid_ids.append(movie_id)

    missing_ids = [movie_id for movie_id in valid_ids if movie_id not in cached_by_id]
    if not missing_ids:
        logger.info("Returning cached payloads for %d movies", len(valid_ids))
        return [cached_by_id[movie_id] for movie_id in valid_ids]

    logger.info("Fetching %d movie id(s) missing from cache: %s", len(missing_ids), missing_ids)
    with requests.Session() as session:
        for movie_id in missing_ids:
            try:
                cached_by_id[movie_id] = fetch_movie_payload(session, movie_id)
            except (ValueError, requests.RequestException):
                if not cached_by_id:
                    logger.info("No cache available and API unreachable; falling back to sample payloads")
                    return json.loads(SAMPLE_PAYLOAD_PATH.read_text(encoding="utf-8"))
                logger.warning("Could not fetch movie id %s; leaving it out of this run", movie_id)

    RAW_PAYLOAD_PATH.parent.mkdir(parents=True, exist_ok=True)
    RAW_PAYLOAD_PATH.write_text(json.dumps(list(cached_by_id.values()), indent=2), encoding="utf-8")
    logger.info("Cached raw payloads at %s (%d movies)", RAW_PAYLOAD_PATH, len(cached_by_id))
    return [cached_by_id[movie_id] for movie_id in valid_ids if movie_id in cached_by_id]
