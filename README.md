# DEM05: Batch & Streaming Data Pipelines (PySpark)

Two independent PySpark data pipeline labs, covering the two main ways data
gets processed in practice: a fixed dataset analyzed all at once (**batch**),
and a continuous flow of events processed as it arrives (**streaming**).
Each lives in its own subdirectory with its own environment, dependencies,
and README -- they don't share code or infrastructure.

| | [Batch](Batch/) | [Streaming](Streaming/) |
|---|---|---|
| **Question it answers** | What happened, across the whole dataset? | What is happening right now? |
| **Input** | TMDB movie API data (or bundled sample payloads), fetched once | Simulated shopper events (views, cart-adds, purchases), generated continuously |
| **Processing** | One-shot PySpark job: clean → analyze → visualize → report | Spark Structured Streaming: watches a folder, cleans and validates each micro-batch as it arrives |
| **Output** | `reports/movie_analysis_report.md` + charts, `data/clean_movies.csv` | Rows continuously inserted into a PostgreSQL `events` table; invalid rows saved to `data/rejected/` |
| **Run once with** | `python run_pipeline.py` | `python data_generator.py` (Terminal 1) + `python spark_streaming_to_postgres.py` (Terminal 2) |

## [Batch/](Batch/) -- TMDB Movie Data Analysis Pipeline

Fetches movie data from The Movie Database (TMDB) API, cleans and
transforms it with PySpark, computes KPI rankings (top/worst movies by
revenue, ROI, rating, etc.), compares franchise vs. standalone performance,
and produces a Markdown report with Matplotlib charts. Runs end-to-end
offline against bundled sample data if no API key is configured.

See [Batch/README.md](Batch/README.md) for setup, configuration, and usage,
and [Batch/Data-Analysis-using-SparknPyspark-I.md](Batch/Data-Analysis-using-SparknPyspark-I.md)
for the original project spec.

## [Streaming/](Streaming/) -- Real-Time E-Commerce Event Pipeline

Simulates shopper activity on an e-commerce site and streams each event
through Spark Structured Streaming into PostgreSQL within seconds of being
generated -- typing and validating every row, and routing invalid rows to
`data/rejected/` instead of dropping them, so nothing disappears without a
trace.

See [Streaming/README.md](Streaming/README.md) for setup and usage,
[Streaming/project_overview.md](Streaming/project_overview.md) for an
architecture walkthrough, and
[Streaming/performance_metrics.md](Streaming/performance_metrics.md) for
throughput/latency numbers from a real run.

## Common prerequisites

Both pipelines are PySpark 3.5 projects and share the same base
requirements, set up independently in each subdirectory:

- Python 3.12 (PySpark 3.5.x does not support newer Python releases)
- Java 8 or 11
- Each subdirectory has its own virtual environment and `requirements.txt`:
  ```bash
  cd Batch  # or Streaming
  python3.12 -m venv .venv
  source .venv/bin/activate
  pip install -r requirements.txt
  ```

The Streaming pipeline additionally requires a running PostgreSQL instance --
see [Streaming/README.md](Streaming/README.md) for setup.
