"""Tests for the PySpark cleaning pipeline in src/tmdb_pipeline/processing.py."""
from src.tmdb_pipeline.config import FINAL_COLUMNS
from src.tmdb_pipeline.processing import clean_movie_data


def _row_by_id(df, movie_id: int) -> dict:
    rows = df.filter(df.id == movie_id).collect()
    assert len(rows) == 1, f"expected exactly one row for id={movie_id}"
    return rows[0].asDict()


def test_clean_movie_data_drops_unreleased_and_sparse_rows(spark, movie_payloads):
    clean_df = clean_movie_data(spark, movie_payloads)
    remaining_ids = {row.id for row in clean_df.select("id").collect()}
    assert remaining_ids == {1, 2, 3, 4}


def test_clean_movie_data_produces_final_column_order(spark, movie_payloads):
    clean_df = clean_movie_data(spark, movie_payloads)
    assert clean_df.columns == FINAL_COLUMNS


def test_json_like_columns_are_flattened_to_pipe_separated_strings(spark, movie_payloads):
    clean_df = clean_movie_data(spark, movie_payloads)
    row = _row_by_id(clean_df, 1)
    assert row["genres"] == "Action|Science Fiction"
    assert row["belongs_to_collection"] == "Test Collection"
    assert row["production_companies"] == "Test Studio"


def test_credits_are_extracted_into_cast_and_director_fields(spark, movie_payloads):
    clean_df = clean_movie_data(spark, movie_payloads)
    row = _row_by_id(clean_df, 1)
    assert row["cast"] == "Bruce Willis|Actor Two"
    assert row["cast_size"] == 2
    assert row["director"] == "Director One"
    assert row["crew_size"] == 1


def test_financial_metrics_are_computed_in_million_usd(spark, movie_payloads):
    clean_df = clean_movie_data(spark, movie_payloads)
    row = _row_by_id(clean_df, 1)
    assert row["budget_musd"] == 100.0
    assert row["revenue_musd"] == 500.0
    assert row["profit_musd"] == 400.0
    assert row["roi"] == 5.0


def test_zero_values_and_placeholder_text_become_null(spark, movie_payloads):
    clean_df = clean_movie_data(spark, movie_payloads)
    row = _row_by_id(clean_df, 4)
    assert row["budget_musd"] is None
    assert row["revenue_musd"] is None
    assert row["tagline"] is None
    assert row["overview"] is None
