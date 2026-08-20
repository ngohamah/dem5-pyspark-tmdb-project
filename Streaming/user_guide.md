# User Guide: Running the Real-Time Event Pipeline

## 1. Prerequisites

- Python 3.12 (PySpark 3.5 is not yet compatible with newer Python releases)
- Java 8 or 11 (required by Spark). **On Apple Silicon, install a native
  `arm64` build** (e.g. [Eclipse Temurin 11](https://adoptium.net/)) --
  Java 8 is commonly only available as an x86_64 build there, which runs
  under Rosetta 2. Since Rosetta translation is inherited by child
  processes, Spark's Python worker subprocess then also runs under x86_64
  emulation, and native Python extensions like `psycopg2` -- which the
  `mapPartitions`-based sink (`sink.py`) imports inside that worker, not
  just in the driver -- fail to load with an "incompatible architecture"
  error.

  Once a native `arm64` JDK is installed (e.g. extracted to
  `~/.jdks/jdk-11.0.32+9`), point your shell at it before running the
  pipeline or the tests:
  ```bash
  export JAVA_HOME="$HOME/.jdks/jdk-11.0.32+9/Contents/Home"
  export PATH="$JAVA_HOME/bin:$PATH"
  ```
  To make this permanent instead of setting it per terminal, append those
  same two lines to `~/.zshrc` (or your shell's equivalent profile).
- PostgreSQL 14+ running locally, or reachable over the network

## 2. Set up the Python environment

```bash
cd Streaming
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Set up PostgreSQL

Run the setup script once, as a Postgres superuser:

```bash
psql -h localhost -p 5432 -U postgres -f postgres_setup.sql
```

This creates the `streaming_app` role, the `streaming_events` database, and
the `events` table. Then give the role a password and record it locally:

```bash
psql -h localhost -p 5432 -U postgres -c "ALTER ROLE streaming_app WITH PASSWORD 'choose-a-password';"
cp .env.example .env
# edit .env and set POSTGRES_PASSWORD to the password you just chose
```

See [postgres_connection_details.txt](postgres_connection_details.txt) for
why the password lives in `.env` and not in a tracked file.

## 4. Run the pipeline

Two processes run independently -- start the generator first so there's
data to stream, then start the Spark job in a second terminal.

**Terminal 1 -- generate events continuously:**
```bash
python data_generator.py
```
Stop it any time with Ctrl+C. Useful flags:
- `--num-files N` -- stop automatically after N files (omit to run forever)
- `--events-per-file N` -- events per file (default 50)
- `--interval SECONDS` -- seconds between files (default 5)
- `--seed N` -- reproducible output, for demos/tests

**Terminal 2 -- stream and load into Postgres:**
```bash
python spark_streaming_to_postgres.py
```
Stop it with Ctrl+C. Useful flags:
- `--once` -- process every file currently in `data/incoming/`, then exit
  (useful for a quick one-shot test instead of a long-running stream)
- `--duration SECONDS` -- run continuously but stop automatically after
  this many seconds (useful for a timed demo)

## 5. Check the results

```bash
psql -h localhost -p 5432 -U streaming_app -d streaming_events -c "SELECT event_type, count(*) FROM events GROUP BY event_type;"
```

- `logs/pipeline.log` -- a full activity log (files written, batches
  processed, rows inserted/rejected, errors)
- `logs/batch_metrics.csv` -- one row per micro-batch with row counts and
  timing, used to generate `performance_metrics.md`
- `data/rejected/rejected_batch_<id>.csv` -- any rows a batch dropped, with
  the reason in the `validation_reason` column

## 6. Run the test suite

```bash
pytest -q
```

Most tests are pure unit tests and always run. The tests in `tests/test_db.py`
talk to the real `streaming_events` database and are automatically skipped
if it isn't reachable.

## 7. Lint

```bash
ruff check .
```
