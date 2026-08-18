"""Shared pytest fixtures for the TMDB pipeline test suite."""
import os
import sys

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("tmdb-pipeline-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def movie_payloads() -> list[dict]:
    """A small, hand-crafted batch covering every cleaning rule under test."""
    def credits(cast_names, director):
        return {
            "cast": [{"name": name, "order": i} for i, name in enumerate(cast_names)],
            "crew": [{"name": director, "job": "Director"}],
        }

    return [
        {
            "id": 1, "title": "Franchise Movie One", "tagline": "Tag one",
            "overview": "Overview one", "release_date": "2015-06-06", "status": "Released",
            "genres": [{"name": "Action"}, {"name": "Science Fiction"}],
            "belongs_to_collection": {"name": "Test Collection"},
            "production_companies": [{"name": "Test Studio"}],
            "production_countries": [{"name": "United States of America"}],
            "spoken_languages": [{"name": "English"}],
            "original_language": "en",
            "budget": 100_000_000, "revenue": 500_000_000, "runtime": 120,
            "vote_count": 1000, "vote_average": 7.5, "popularity": 50.0,
            "poster_path": "/p1.jpg",
            "credits": credits(["Bruce Willis", "Actor Two"], "Director One"),
        },
        {
            "id": 2, "title": "Franchise Movie Two", "tagline": "Tag two",
            "overview": "Overview two", "release_date": "2018-06-06", "status": "Released",
            "genres": [{"name": "Action"}, {"name": "Adventure"}],
            "belongs_to_collection": {"name": "Test Collection"},
            "production_companies": [{"name": "Test Studio"}],
            "production_countries": [{"name": "United States of America"}],
            "spoken_languages": [{"name": "English"}],
            "original_language": "en",
            "budget": 150_000_000, "revenue": 300_000_000, "runtime": 130,
            "vote_count": 500, "vote_average": 6.0, "popularity": 30.0,
            "poster_path": "/p2.jpg",
            "credits": credits(["Actor Three"], "Director Two"),
        },
        {
            "id": 3, "title": "Standalone Tarantino Movie", "tagline": "Tag three",
            "overview": "Overview three", "release_date": "2003-10-10", "status": "Released",
            "genres": [{"name": "Crime"}, {"name": "Thriller"}],
            "belongs_to_collection": None,
            "production_companies": [{"name": "Indie Studio"}],
            "production_countries": [{"name": "United States of America"}],
            "spoken_languages": [{"name": "English"}],
            "original_language": "en",
            "budget": 30_000_000, "revenue": 180_000_000, "runtime": 111,
            "vote_count": 800, "vote_average": 8.0, "popularity": 45.0,
            "poster_path": "/p3.jpg",
            "credits": credits(["Uma Thurman"], "Quentin Tarantino"),
        },
        {
            # Zero budget/revenue/runtime + placeholder text -> should become null.
            "id": 4, "title": "Zero Budget Movie", "tagline": "No Data",
            "overview": "No Data", "release_date": "2020-01-01", "status": "Released",
            "genres": [{"name": "Drama"}],
            "belongs_to_collection": None,
            "production_companies": [{"name": "Unknown Studio"}],
            "production_countries": [{"name": "United States of America"}],
            "spoken_languages": [{"name": "English"}],
            "original_language": "en",
            "budget": 0, "revenue": 0, "runtime": 0,
            "vote_count": 0, "vote_average": 0.0, "popularity": 1.0,
            "poster_path": "/p4.jpg",
            "credits": credits(["Actor Four"], "Director Three"),
        },
        {
            # Not released -> should be dropped entirely.
            "id": 5, "title": "Unreleased Movie", "tagline": "Coming soon",
            "overview": "Overview five", "release_date": "2030-01-01", "status": "Post Production",
            "genres": [{"name": "Drama"}],
            "belongs_to_collection": None,
            "production_companies": [{"name": "Unknown Studio"}],
            "production_countries": [{"name": "United States of America"}],
            "spoken_languages": [{"name": "English"}],
            "original_language": "en",
            "budget": 1_000_000, "revenue": 0, "runtime": 90,
            "vote_count": 0, "vote_average": 0.0, "popularity": 1.0,
            "poster_path": "/p5.jpg",
            "credits": credits(["Actor Five"], "Director Four"),
        },
        {
            # Too few populated columns -> should be dropped by the sparsity rule.
            "id": 6, "title": "Sparse Movie", "status": "Released",
        },
    ]
