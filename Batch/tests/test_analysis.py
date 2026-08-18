"""Tests for KPI rankings and summaries in src/tmdb_pipeline/analysis.py."""
import pytest

from src.tmdb_pipeline.analysis import (
    build_rankings,
    compare_franchise_vs_standalone,
    rank_movies,
    search_sci_fi_action_bruce_willis,
    search_uma_thurman_quentin_tarantino,
    summarize_director_performance,
    summarize_franchise_performance,
    summarize_yearly_trends,
)
from src.tmdb_pipeline.processing import clean_movie_data


@pytest.fixture
def clean_df(spark, movie_payloads):
    return clean_movie_data(spark, movie_payloads)


def test_rank_movies_orders_descending_and_respects_top_n(clean_df):
    # Revenue (musd): id1=500, id2=300, id3=180, id4=null (nulls sort last).
    ranked = rank_movies(clean_df, "revenue_musd", ascending=False, top_n=2).collect()
    assert [row.id for row in ranked] == [1, 2]
    assert [row.rank for row in ranked] == [1, 2]


def test_rank_movies_orders_ascending(clean_df):
    ranked = rank_movies(clean_df, "revenue_musd", ascending=True, top_n=1).collect()
    assert ranked[0].id == 3


def test_rank_movies_rejects_unknown_metric(clean_df):
    with pytest.raises(KeyError):
        rank_movies(clean_df, "not_a_real_column")


def test_build_rankings_returns_all_ten_kpis(clean_df):
    rankings = build_rankings(clean_df)
    expected_keys = {
        "highest_revenue", "highest_budget", "highest_profit", "lowest_profit",
        "highest_roi", "lowest_roi", "most_voted", "highest_rated",
        "lowest_rated", "most_popular",
    }
    assert set(rankings.keys()) == expected_keys


def test_search_sci_fi_action_bruce_willis_matches_only_qualifying_movie(clean_df):
    results = search_sci_fi_action_bruce_willis(clean_df).collect()
    assert [row.id for row in results] == [1]


def test_search_uma_thurman_quentin_tarantino_matches_expected_movie(clean_df):
    results = search_uma_thurman_quentin_tarantino(clean_df).collect()
    assert [row.id for row in results] == [3]


def test_compare_franchise_vs_standalone_has_two_groups(clean_df):
    comparison = compare_franchise_vs_standalone(clean_df).collect()
    assert {row.is_franchise for row in comparison} == {True, False}


def test_summarize_franchise_performance_counts_collection_movies(clean_df):
    summary = summarize_franchise_performance(clean_df).collect()
    assert len(summary) == 1
    assert summary[0].belongs_to_collection == "Test Collection"
    assert summary[0].movie_count == 2


def test_summarize_director_performance_ranks_by_revenue(clean_df):
    summary = summarize_director_performance(clean_df).collect()
    directors = [row.director for row in summary]
    assert directors[0] == "Director One"  # highest revenue movie


def test_summarize_yearly_trends_groups_by_release_year(clean_df):
    trends = summarize_yearly_trends(clean_df).collect()
    years = [row.release_year for row in trends]
    assert years == sorted(years)
    assert 2015 in years
