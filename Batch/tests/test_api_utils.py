"""Tests for src/tmdb_pipeline/api_utils.py (no real network calls are made)."""
import json

import pytest
import requests

from src.tmdb_pipeline import api_utils


def test_fetch_movie_payload_raises_without_a_configured_api_key():
    with pytest.raises(ValueError):
        api_utils.fetch_movie_payload(requests.Session(), 123)


def test_fetch_movie_batch_skips_non_positive_ids(tmp_path, monkeypatch):
    cache_path = tmp_path / "raw_payloads.json"
    cache_path.write_text(json.dumps([{"id": 1, "title": "Movie One"}]), encoding="utf-8")
    monkeypatch.setattr(api_utils, "RAW_PAYLOAD_PATH", cache_path)

    result = api_utils.fetch_movie_batch([0, -5, 1])

    assert [payload["id"] for payload in result] == [1]


def test_fetch_movie_batch_uses_cache_without_hitting_the_api(tmp_path, monkeypatch):
    cache_path = tmp_path / "raw_payloads.json"
    cache_path.write_text(json.dumps([{"id": 42, "title": "Cached Movie"}]), encoding="utf-8")
    monkeypatch.setattr(api_utils, "RAW_PAYLOAD_PATH", cache_path)

    def _fail_if_called(*args, **kwargs):
        raise AssertionError("should not hit the network when the cache already has the id")

    monkeypatch.setattr(requests.Session, "get", _fail_if_called)

    result = api_utils.fetch_movie_batch([42])

    assert result == [{"id": 42, "title": "Cached Movie"}]


def test_fetch_movie_batch_falls_back_to_sample_payloads_when_unreachable(tmp_path, monkeypatch):
    empty_cache = tmp_path / "raw_payloads.json"  # does not exist -> empty cache
    sample_path = tmp_path / "sample_payloads.json"
    sample_path.write_text(json.dumps([{"id": 7, "title": "Sample Movie"}]), encoding="utf-8")

    monkeypatch.setattr(api_utils, "RAW_PAYLOAD_PATH", empty_cache)
    monkeypatch.setattr(api_utils, "SAMPLE_PAYLOAD_PATH", sample_path)
    monkeypatch.setattr(api_utils, "TMDB_API_KEY", "YOUR_TMDB_API_KEY")  # unconfigured placeholder

    result = api_utils.fetch_movie_batch([7])

    assert result == [{"id": 7, "title": "Sample Movie"}]
