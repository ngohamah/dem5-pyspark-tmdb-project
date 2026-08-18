# TMDB Movie Data Analysis Pipeline (PySpark)

An end-to-end batch pipeline that fetches movie data from [The Movie Database (TMDB)](https://www.themoviedb.org/) API, cleans and transforms it with PySpark, computes KPI rankings and performance summaries (top/worst movies, franchise vs. standalone comparisons, franchise/director performance), and produces a Markdown report with supporting Matplotlib charts.

Built to the spec in `Data-Analysis-using-SparknPyspark-I.md`.

## What it does

Running the pipeline (`run_pipeline.py`) performs these steps in order:

1. **Fetch** — Requests a fixed batch of movies (by TMDB ID, see `src/tmdb_pipeline/config.py`) from the TMDB API in a single session, with `append_to_response=credits` so cast/crew come back in the same request (one HTTP call per movie — no follow-up requests, no rate-limit risk). If no API key is configured or the request fails, it falls back to the bundled sample payloads (`data/sample_payloads.json`) so the pipeline still runs end-to-end offline. Raw responses are cached to `data/raw_payloads.json`; re-running the pipeline reuses the cache instead of re-hitting the API.
2. **Clean** — Loads the raw payloads into a Spark DataFrame and, as a functional chain of column expressions (see `processing.pipe`): drops irrelevant columns, flattens JSON-like struct/array fields (genres, production companies/countries, spoken languages, collections) using Spark's native higher-order array functions (`transform`, `filter` — no per-row Python UDFs), extracts cast/director/crew from the nested `credits` field, fixes data types, treats zero budget/revenue/runtime and placeholder text as missing, converts budget/revenue to million-USD units, derives `profit_musd`/`roi`, removes duplicates/incomplete rows, and keeps only `Released` movies. The result is saved to `data/clean_movies.csv`.
3. **Analyze** — Builds top-10 rankings (highest revenue, budget, profit, ROI, votes, rating, popularity, etc.) using a single reusable `rank_movies` routine built on a Spark `Window` + a `udf` that labels each row's rank (e.g. "Top 3") for readability. Runs the two required searches (Bruce Willis sci-fi/action movies, Tarantino/Uma Thurman movies), compares franchise vs. standalone performance, and ranks the most successful franchises and directors.
4. **Visualize** — Saves 5 labeled charts to `reports/plots/`: Revenue vs. Budget, ROI by Genre, Popularity vs. Rating, Yearly Box Office Trends, and Franchise vs. Standalone Performance. Each chart carries a plain-language title and axis labels aimed at non-technical readers.
5. **Report** — Writes a combined Markdown summary to `reports/movie_analysis_report.md`.

All steps log progress and errors to `logs/pipeline.log`, including every row-count change (dropped duplicates, dropped sparse rows, dropped non-Released movies) for traceability.

## Project structure

```
run_pipeline.py                 Entry point — runs the full pipeline
src/tmdb_pipeline/
  config.py                     Paths, Spark/TMDB settings, movie ID list, column definitions
  api_utils.py                  TMDB API fetching + cached/sample payload fallback
  spark_utils.py                SparkSession creation
  processing.py                 PySpark data cleaning & transformation
  analysis.py                   KPI rankings, searches, franchise/director summaries
  visualization.py               Chart generation (matplotlib, via toPandas())
  logger_config.py              Shared file-based logging setup
tests/                          pytest suite (Spark-backed, no real network calls)
data/                           Raw and cleaned datasets (generated + sample data)
reports/                        Generated report and plots
logs/                           Pipeline log output
requirements.txt                Python dependencies
```

## Prerequisites

- Python 3.11 or 3.12 (PySpark 3.5.x is not compatible with 3.13+; the bundled `.venv` uses 3.12)
- Java 8, 11, or 17 (PySpark 3.5.x requirement — this project was verified against Java 8)
- A [TMDB API key](https://www.themoviedb.org/settings/api) — optional. Without one, the pipeline uses the bundled sample data automatically.

## Installation

1. Create and activate a virtual environment with a supported Python version:
   ```bash
   python3.12 -m venv .venv
   source .venv/bin/activate      # Windows: .venv\Scripts\activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

   > If `toPandas()` fails with `ModuleNotFoundError: No module named 'distutils'` (Python 3.12 removed it from the standard library), run `pip install "setuptools>=60"` — it registers a compatibility shim PySpark 3.5.x relies on.

## Configuration (optional)

To fetch live data instead of using the sample payloads, set your TMDB API key in `src/tmdb_pipeline/config.py`:

```python
TMDB_API_KEY = "YOUR_TMDB_API_KEY"
```

Replace the placeholder with your real key. If it's left as-is, or a request fails, the pipeline logs a warning and transparently falls back to `data/sample_payloads.json`.

> Note: `data/raw_payloads.json`, if present, is treated as a cache and reused instead of hitting the API again. Delete it to force a fresh fetch.

## Running the pipeline

From the project root, with the virtual environment activated:

```bash
python run_pipeline.py
```

This generates/updates:
- `data/clean_movies.csv` — cleaned dataset
- `reports/movie_analysis_report.md` — rankings and summary report
- `reports/plots/*.png` — generated charts
- `logs/pipeline.log` — execution log

## Testing

```bash
pytest
```

Tests spin up a local SparkSession and exercise the cleaning pipeline, KPI rankings/searches/summaries, and the API caching/fallback logic (mocked — no real network calls).

## Linting

```bash
ruff check .
```
