"""Run the TMDB movie analysis pipeline (PySpark)."""
from __future__ import annotations

from src.tmdb_pipeline.analysis import (
    build_rankings,
    compare_franchise_vs_standalone,
    search_sci_fi_action_bruce_willis,
    search_uma_thurman_quentin_tarantino,
    summarize_director_performance,
    summarize_franchise_performance,
    summarize_yearly_trends,
)
from src.tmdb_pipeline.api_utils import fetch_movie_batch
from src.tmdb_pipeline.config import CLEAN_DATA_PATH, PLOTS_DIR, REPORT_PATH
from src.tmdb_pipeline.logger_config import configure_logger
from src.tmdb_pipeline.processing import clean_movie_data, write_single_csv
from src.tmdb_pipeline.spark_utils import get_spark_session
from src.tmdb_pipeline.visualization import save_plots

logger = configure_logger("run_pipeline")


def _section(title: str, pdf, max_rows: int = 10) -> list[str]:
    """Render a titled table section for the Markdown report."""
    return [f"### {title}", "", pdf.head(max_rows).to_string(index=False), ""]


def main() -> None:
    """Execute the full end-to-end pipeline: fetch, clean, analyze, visualize, report."""
    spark = get_spark_session()
    try:
        payloads = fetch_movie_batch()

        clean_df = clean_movie_data(spark, payloads)
        clean_df.cache()
        write_single_csv(clean_df, CLEAN_DATA_PATH)

        rankings = build_rankings(clean_df)
        search_bruce_willis = search_sci_fi_action_bruce_willis(clean_df)
        search_tarantino = search_uma_thurman_quentin_tarantino(clean_df)
        franchise_vs_standalone = compare_franchise_vs_standalone(clean_df)
        franchise_summary = summarize_franchise_performance(clean_df)
        director_summary = summarize_director_performance(clean_df)
        yearly_trends = summarize_yearly_trends(clean_df)

        clean_pdf = clean_df.toPandas()
        franchise_vs_standalone_pdf = franchise_vs_standalone.toPandas()
        yearly_trends_pdf = yearly_trends.toPandas()

        plot_paths = save_plots(clean_pdf, franchise_vs_standalone_pdf, yearly_trends_pdf, PLOTS_DIR)

        report_lines = [
            "# TMDB Movie Data Analysis Report",
            "",
            "## Summary",
            f"- Fetched and cleaned {clean_pdf.shape[0]} released movies from TMDB.",
            "- Ranked movies by revenue, budget, profit, ROI, votes, rating, and popularity.",
            "- Compared franchise vs. standalone performance and ranked top franchises/directors.",
            "",
            "## Top Rankings",
            "",
        ]
        for name, ranked_df in rankings.items():
            report_lines.extend(_section(name.replace("_", " ").title(), ranked_df.toPandas(), max_rows=5))

        report_lines.extend(_section("Best-Rated Sci-Fi Action Movies Starring Bruce Willis", search_bruce_willis.toPandas()))
        report_lines.extend(_section("Uma Thurman Movies Directed by Quentin Tarantino", search_tarantino.toPandas()))
        report_lines.extend(_section("Franchise vs. Standalone Performance", franchise_vs_standalone_pdf))
        report_lines.extend(_section("Most Successful Franchises", franchise_summary.toPandas()))
        report_lines.extend(_section("Most Successful Directors", director_summary.toPandas()))
        report_lines.extend(_section("Yearly Box Office Trends", yearly_trends_pdf, max_rows=len(yearly_trends_pdf)))

        report_lines.append("## Charts")
        report_lines.append("")
        for path in plot_paths:
            report_lines.append(f"- `{path.relative_to(REPORT_PATH.parent)}`")

        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text("\n".join(report_lines), encoding="utf-8")
        logger.info("Wrote report to %s", REPORT_PATH)
    except Exception:
        logger.exception("Pipeline execution failed")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
