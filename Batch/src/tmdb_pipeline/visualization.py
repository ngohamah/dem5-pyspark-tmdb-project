"""Matplotlib chart generation for the TMDB analysis.

Spark DataFrames are collected to the driver as small, already-aggregated
pandas frames before plotting -- matplotlib has no notion of a distributed
DataFrame, so this is the expected hand-off point between the two.
Every chart carries a plain-language title and labeled axes so the report
reads clearly for non-technical stakeholders.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from .logger_config import configure_logger

logger = configure_logger(__name__)


def _studio_color_map(plot_df: pd.DataFrame) -> tuple[pd.Series, list, plt.Colormap, plt.Normalize]:
    primary_studio = plot_df["production_companies"].fillna("Unknown").str.split("|").str[0]
    studio_codes, studio_labels = pd.factorize(primary_studio)
    cmap = plt.get_cmap("tab20")
    norm = plt.Normalize(vmin=0, vmax=max(len(studio_labels) - 1, 1))
    handles = [
        plt.Line2D([], [], marker="o", linestyle="", color=cmap(norm(code)), label=label)
        for code, label in enumerate(studio_labels)
    ]
    return studio_codes, handles, cmap, norm


def plot_revenue_vs_budget(plot_df: pd.DataFrame, output_dir: Path) -> Path:
    """Scatter plot: does a bigger budget translate into bigger box-office revenue?"""
    codes, handles, cmap, norm = _studio_color_map(plot_df)
    fig, ax = plt.subplots(figsize=(12, 9))
    ax.scatter(plot_df["budget_musd"], plot_df["revenue_musd"], c=codes, cmap=cmap, norm=norm, alpha=0.7)
    ax.set_title("Do Bigger Budgets Lead to Bigger Box Office Revenue?")
    ax.set_xlabel("Budget (Million USD)")
    ax.set_ylabel("Revenue (Million USD)")
    ax.legend(handles=handles, title="Production Company", bbox_to_anchor=(1.02, 1), loc="upper left", fontsize="small")
    path = output_dir / "revenue_vs_budget.png"
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_roi_by_genre(plot_df: pd.DataFrame, output_dir: Path) -> Path:
    """Boxplot of return-on-investment grouped by each movie's primary (first-listed) genre."""
    genre_df = plot_df.copy()
    genre_df["primary_genre"] = genre_df["genres"].fillna("Unknown").str.split("|").str[0]
    fig, ax = plt.subplots(figsize=(14, 8))
    genre_df.boxplot(column="roi", by="primary_genre", ax=ax)
    ax.set_title("Return on Investment by Primary Genre")
    ax.set_xlabel("Primary Genre")
    ax.set_ylabel("ROI (Revenue / Budget, as a multiple)")
    plt.suptitle("")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
    path = output_dir / "roi_by_genre.png"
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return path


def plot_popularity_vs_rating(plot_df: pd.DataFrame, output_dir: Path) -> Path:
    """Scatter plot: are more popular movies also more highly rated?"""
    codes, handles, cmap, norm = _studio_color_map(plot_df)
    fig, ax = plt.subplots(figsize=(12, 9))
    ax.scatter(plot_df["popularity"], plot_df["vote_average"], c=codes, cmap=cmap, norm=norm, alpha=0.7)
    ax.set_title("Are More Popular Movies Rated Higher by Audiences?")
    ax.set_xlabel("Popularity Score (TMDB, higher = more popular)")
    ax.set_ylabel("Average Audience Rating (out of 10)")
    ax.legend(handles=handles, title="Production Company", bbox_to_anchor=(1.02, 1), loc="upper left", fontsize="small")
    path = output_dir / "popularity_vs_rating.png"
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_yearly_trends(yearly_pdf: pd.DataFrame, output_dir: Path) -> Path:
    """Line chart: average budget and revenue by release year."""
    fig, ax = plt.subplots(figsize=(12, 7))
    ax.plot(yearly_pdf["release_year"], yearly_pdf["mean_budget_musd"], marker="o", label="Average Budget")
    ax.plot(yearly_pdf["release_year"], yearly_pdf["mean_revenue_musd"], marker="o", label="Average Revenue")
    ax.set_title("Yearly Trends in Box Office Performance")
    ax.set_xlabel("Release Year")
    ax.set_ylabel("Amount (Million USD)")
    ax.legend(title="Metric")
    path = output_dir / "yearly_trends.png"
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return path


def plot_franchise_vs_standalone(comparison_pdf: pd.DataFrame, output_dir: Path) -> Path:
    """Bar chart comparing franchise movies vs. standalone movies on key metrics."""
    labels = comparison_pdf["is_franchise"].map({True: "Franchise", False: "Standalone"})
    metrics = [
        ("mean_revenue_musd", "Avg Revenue (M USD)"),
        ("mean_budget_musd", "Avg Budget (M USD)"),
        ("mean_popularity", "Avg Popularity"),
        ("mean_rating", "Avg Rating (/10)"),
    ]

    fig, axes = plt.subplots(1, len(metrics), figsize=(16, 5))
    for ax, (column, label) in zip(axes, metrics):
        ax.bar(labels, comparison_pdf[column], color=["#4C72B0", "#DD8452"])
        ax.set_title(label)
        ax.set_xlabel("Movie Type")
    fig.suptitle("Franchise vs. Standalone Movie Performance")
    path = output_dir / "franchise_vs_standalone.png"
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return path


def save_plots(
    clean_pdf: pd.DataFrame,
    franchise_vs_standalone_pdf: pd.DataFrame,
    yearly_trends_pdf: pd.DataFrame,
    output_dir: Path,
) -> list[Path]:
    """Generate and save every chart called for by the project spec."""
    output_dir.mkdir(parents=True, exist_ok=True)

    plot_df = clean_pdf.dropna(subset=["budget_musd", "revenue_musd", "roi"]).copy()

    try:
        saved_paths = [
            plot_revenue_vs_budget(plot_df, output_dir),
            plot_roi_by_genre(plot_df, output_dir),
            plot_popularity_vs_rating(plot_df, output_dir),
            plot_yearly_trends(yearly_trends_pdf, output_dir),
            plot_franchise_vs_standalone(franchise_vs_standalone_pdf, output_dir),
        ]
        logger.info("Saved %d plots to %s", len(saved_paths), output_dir)
        return saved_paths
    except Exception:
        logger.exception("Plot generation failed")
        raise
