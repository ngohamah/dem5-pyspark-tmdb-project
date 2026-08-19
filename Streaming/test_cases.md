# Test Plan: Real-Time Event Pipeline

Two kinds of testing back this project:

- **Automated tests** (`pytest -q`, 31 tests) cover the pure logic (event
  generation, file writing, type casting, validation rules, the Postgres
  sink) in isolation. Run them anytime with `pytest -q`; the results below
  are from the run in this repo's history.
- **Manual/system tests** below cover the "What to Test" checklist from the
  project brief -- end-to-end behavior that only shows up when the
  generator, Spark, and Postgres are all running together.

## Automated test summary

| Suite | What it checks | Result |
|---|---|---|
| `tests/test_events.py` | Event generation, seeded reproducibility, injected defects | 10/10 passed |
| `tests/test_generator_io.py` | Atomic CSV writes, no partial files, empty-batch handling | 4/4 passed |
| `tests/test_transform.py` | Type casting and every validation rule, individually and combined | 10/10 passed |
| `tests/test_sink.py` | Valid/invalid row routing, row-count logging (Postgres mocked) | 3/3 passed |
| `tests/test_db.py` | Real inserts, duplicate handling, connection-failure logging | 4/4 passed (auto-skips if Postgres is unreachable) |

## Manual / system test plan

| ID | Description | Steps | Expected result | Actual result | Status |
|---|---|---|---|---|---|
| TC-01 | CSV files are generated correctly | Run `data_generator.py --num-files 5 --events-per-file 8` | 5 CSV files appear in `data/incoming/`, each with a header row and 8 data rows matching the documented columns | Confirmed: 5 files, correct header, correct row counts | Pass |
| TC-02 | Generated files are never partially visible | Inspect `data/incoming/` while the generator is writing | No `.tmp_*` files ever appear in the watched directory | Confirmed via `write_events_csv` (write-then-rename) and its tests | Pass |
| TC-03 | A small, known fraction of events is intentionally invalid | Generate a large batch (2000 events) and inspect for injected defects | Some but not most events have a negative price, missing product id, unknown type, bad timestamp, or negative quantity | Confirmed (~5% of a 2000-event batch) | Pass |
| TC-04 | Spark detects and processes new files as they arrive (continuous mode) | Run the generator and `spark_streaming_to_postgres.py` at the same time for 90 seconds | Each new CSV file is picked up and processed within one trigger interval, without restarting the job | Confirmed: 18 micro-batches processed as 18 files arrived; see `performance_metrics.md` | Pass |
| TC-05 | `--once` mode drains the backlog and exits | Generate 3 files, then run `spark_streaming_to_postgres.py --once` | All 3 files are processed in one run; the process then exits on its own | Confirmed in initial smoke test | Pass |
| TC-06 | Data transformations are correct (type conversion) | Feed rows with string `"42"`, `"19.99"`, ISO timestamps through `clean_events` | `user_id` becomes an int, `price` a double, `event_time` a timestamp; malformed values become null instead of crashing | Confirmed via `tests/test_transform.py` | Pass |
| TC-07 | Invalid rows are flagged with the correct reason and excluded from Postgres | Include known-bad rows (bad price, missing product id, etc.) in a batch | Each bad row gets exactly one `validation_reason` and `is_valid=False`; good rows are unaffected | Confirmed: real run's rejected-file counts (31) match `is_valid=False` counts exactly | Pass |
| TC-08 | Valid data is written into PostgreSQL without errors | Run the full pipeline for 90 seconds; compare row counts | `SELECT count(*) FROM events` equals the sink's own "inserted" total | Confirmed: 509 rows inserted, `SELECT count(*)` = 509 | Pass |
| TC-09 | Re-processing a batch doesn't duplicate rows | Insert the same event twice via `insert_events` | Second insert reports 0 rows inserted (ON CONFLICT DO NOTHING) | Confirmed via `tests/test_db.py::test_insert_events_skips_duplicate_event_ids` | Pass |
| TC-10 | Rejected rows are saved, not discarded | Run the pipeline with some malformed input; inspect `data/rejected/` | One CSV per batch with rejected rows, each row carrying its own `validation_reason` | Confirmed: 15 rejected-batch files present, totals reconcile with the sink log | Pass |
| TC-11 | Performance metrics are captured per batch and stay within reasonable bounds | Run the pipeline for 90 seconds; inspect `logs/batch_metrics.csv` and `logs/batch_row_counts.csv` | Every batch logs its duration and throughput; steady-state batch duration stays well under the 5-second trigger interval | Confirmed: batches averaged 1.50s (well under 5s) after the one-time startup batch; see `performance_metrics.md` | Pass |
| TC-12 | A database outage is logged, not silently swallowed | Point `POSTGRES_HOST`/`POSTGRES_PORT` at an address nothing is listening on, then call `insert_events` | The failure raises `psycopg2.OperationalError` **and** is written to `logs/pipeline.log` before propagating | Confirmed manually and via `tests/test_db.py::test_insert_events_raises_and_logs_when_the_database_is_unreachable` (this exact scenario surfaced a real bug -- the connection call was originally outside the logged `try` block, fixed in the `db.py` connection-logging commit) | Pass |
| TC-13 | Linting is clean | Run `ruff check .` | No errors | Confirmed | Pass |

## Known limitations (by design, not defects)

- This is a single-machine demo (`local[*]` Spark, one Postgres instance); absolute
  throughput numbers won't reflect a production cluster.
- The pipeline does not automatically retry a failed micro-batch against a
  down database -- it logs the failure and stops (a supervisor/orchestrator
  restarting the job is the expected recovery path, matching how Spark
  Structured Streaming jobs are typically operated).
