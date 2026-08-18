"""KPI rankings and performance summaries over the cleaned TMDB dataset."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from .config import MIN_BUDGET_FOR_ROI_MUSD, MIN_VOTES_FOR_RATING
from .logger_config import configure_logger

logger = configure_logger(__name__)


@F.udf(returnType=StringType())
def _rank_label(rank: int, total: int) -> str:
    """Turn a numeric rank into a plain-language label for non-technical readers."""
    if rank <= 3:
        return "Top 3"
    if total - rank < 3:
        return "Bottom 3"
    return "Top 10"


def rank_movies(df: DataFrame, metric: str, ascending: bool = False, top_n: int = 10) -> DataFrame:
    """Rank movies by ``metric`` using a window function, labeled via a UDF.

    This is the project's single reusable ranking routine -- every KPI in
    :func:`build_rankings` calls it instead of re-implementing sort/limit.
    """
    if metric not in df.columns:
        raise KeyError(f"Metric '{metric}' not found in dataframe")

    order_col = F.col(metric).asc_nulls_last() if ascending else F.col(metric).desc_nulls_last()
    window = Window.orderBy(order_col)

    total = df.count()
    ranked = df.withColumn("rank", F.row_number().over(window))
    ranked = ranked.withColumn("rank_label", _rank_label(F.col("rank"), F.lit(total)))
    return ranked.filter(F.col("rank") <= top_n).orderBy("rank")


def build_rankings(df: DataFrame) -> dict[str, DataFrame]:
    """Build the ten headline KPI rankings called for by the project spec."""
    logger.info("Building KPI rankings")
    well_funded = df.filter(F.col("budget_musd") >= MIN_BUDGET_FOR_ROI_MUSD)
    well_voted = df.filter(F.col("vote_count") >= MIN_VOTES_FOR_RATING)

    return {
        "highest_revenue": rank_movies(df, "revenue_musd", ascending=False),
        "highest_budget": rank_movies(df, "budget_musd", ascending=False),
        "highest_profit": rank_movies(df, "profit_musd", ascending=False),
        "lowest_profit": rank_movies(df, "profit_musd", ascending=True),
        "highest_roi": rank_movies(well_funded, "roi", ascending=False),
        "lowest_roi": rank_movies(well_funded, "roi", ascending=True),
        "most_voted": rank_movies(df, "vote_count", ascending=False),
        "highest_rated": rank_movies(well_voted, "vote_average", ascending=False),
        "lowest_rated": rank_movies(well_voted, "vote_average", ascending=True),
        "most_popular": rank_movies(df, "popularity", ascending=False),
    }


def search_sci_fi_action_bruce_willis(df: DataFrame) -> DataFrame:
    """Best-rated Science Fiction Action movies starring Bruce Willis."""
    return (
        df.filter(
            F.col("genres").contains("Science Fiction")
            & F.col("genres").contains("Action")
            & F.col("cast").contains("Bruce Willis")
        )
        .orderBy(F.col("vote_average").desc_nulls_last())
    )


def search_uma_thurman_quentin_tarantino(df: DataFrame) -> DataFrame:
    """Movies starring Uma Thurman, directed by Quentin Tarantino, shortest runtime first."""
    return (
        df.filter(F.col("cast").contains("Uma Thurman") & (F.col("director") == "Quentin Tarantino"))
        .orderBy(F.col("runtime").asc_nulls_last())
    )


def compare_franchise_vs_standalone(df: DataFrame) -> DataFrame:
    """Compare franchise entries vs. standalone movies on the five headline metrics."""
    return (
        df.withColumn("is_franchise", F.col("belongs_to_collection").isNotNull())
        .groupBy("is_franchise")
        .agg(
            F.mean("revenue_musd").alias("mean_revenue_musd"),
            F.expr("percentile_approx(roi, 0.5)").alias("median_roi"),
            F.mean("budget_musd").alias("mean_budget_musd"),
            F.mean("popularity").alias("mean_popularity"),
            F.mean("vote_average").alias("mean_rating"),
        )
        .orderBy(F.col("is_franchise").desc())
    )


def summarize_franchise_performance(df: DataFrame) -> DataFrame:
    """Rank the movie franchises (collections) by overall box-office success."""
    return (
        df.filter(F.col("belongs_to_collection").isNotNull())
        .groupBy("belongs_to_collection")
        .agg(
            F.count("id").alias("movie_count"),
            F.sum("budget_musd").alias("total_budget_musd"),
            F.mean("budget_musd").alias("mean_budget_musd"),
            F.sum("revenue_musd").alias("total_revenue_musd"),
            F.mean("revenue_musd").alias("mean_revenue_musd"),
            F.mean("vote_average").alias("mean_rating"),
        )
        .orderBy(F.col("total_revenue_musd").desc_nulls_last())
    )


def summarize_director_performance(df: DataFrame) -> DataFrame:
    """Rank directors by total revenue and average rating across their movies."""
    return (
        df.filter(F.col("director").isNotNull())
        .groupBy("director")
        .agg(
            F.count("id").alias("movie_count"),
            F.sum("revenue_musd").alias("total_revenue_musd"),
            F.mean("vote_average").alias("mean_rating"),
        )
        .orderBy(F.col("total_revenue_musd").desc_nulls_last())
    )


def summarize_yearly_trends(df: DataFrame) -> DataFrame:
    """Summarize box-office performance by release year for trend charts."""
    return (
        df.withColumn("release_year", F.year("release_date"))
        .filter(F.col("release_year").isNotNull())
        .groupBy("release_year")
        .agg(
            F.count("id").alias("movie_count"),
            F.mean("budget_musd").alias("mean_budget_musd"),
            F.mean("revenue_musd").alias("mean_revenue_musd"),
        )
        .orderBy("release_year")
    )
