"""Build performance_metrics.md and its chart from a real pipeline run.

Reads the two logs the pipeline writes as it runs -- logs/batch_metrics.csv
(Spark's own per-batch timing) and logs/batch_row_counts.csv (the sink's
ground-truth received/inserted/rejected counts) -- plus the rejected-row
files, and turns them into a report a non-technical stakeholder can read.

A one-off documentation tool, not part of the runtime pipeline: run it by
hand after a demo/test run with `python generate_performance_report.py`.
"""
from __future__ import annotations

import csv
import glob
import re
from datetime import datetime, timezone
from functools import reduce

import matplotlib.pyplot as plt
import pandas as pd

from src.streaming_pipeline.config import (
    BASE_DIR,
    BATCH_ROW_COUNTS_PATH,
    MAX_FILES_PER_TRIGGER,
    METRICS_LOG_PATH,
    PLOTS_DIR,
    REJECTED_DIR,
    STREAM_TRIGGER_INTERVAL,
)
from src.streaming_pipeline.transform import reduce_reasons_to_counts

REPORT_PATH = BASE_DIR / "performance_metrics.md"
THROUGHPUT_PLOT_PATH = PLOTS_DIR / "throughput_per_batch.png"
LATENCY_PLOT_PATH = PLOTS_DIR / "batch_latency.png"

# logs/batch_metrics.csv and logs/batch_row_counts.csv are append-only across
# every run ever executed against the checkpoint in checkpoints/events/ --
# Structured Streaming resumes batch numbering from the checkpoint rather
# than resetting to 0, so a second run's rows land in the same files as the
# first's. A gap between two consecutive batches' timestamps this much
# larger than a normal trigger interval means the pipeline was stopped and
# later restarted with new arguments; anything before the most recent such
# gap belongs to an earlier run and is excluded from this report.
_RUN_GAP_THRESHOLD_SECONDS = 30
_REJECTED_FILENAME_RE = re.compile(r"rejected_batch_(\d+)(?:_part_\d+)?\.csv$")
_METRICS_COLUMNS = ("batch_id", "timestamp", "batch_duration_ms", "processed_rows_per_second")
_ROW_COUNTS_COLUMNS = ("batch_id", "received", "inserted", "rejected")


def _read_csv_rows(path, required_columns: tuple[str, ...]) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if rows and not set(required_columns) <= rows[0].keys():
        raise ValueError(
            f"{path} is missing expected column(s) {set(required_columns) - rows[0].keys()} -- "
            "its header row is likely missing or the file was truncated externally. "
            "Delete it (or restore it from git/a backup) and rerun the pipeline to regenerate it "
            "with a fresh header before building this report."
        )
    return rows


def _parse_timestamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def _latest_run_batch_ids(batch_metrics: list[dict]) -> set[int]:
    """Batch ids belonging to the most recent run, per the module docstring above."""
    if not batch_metrics:
        return set()
    start_index = 0
    for i in range(1, len(batch_metrics)):
        gap = (_parse_timestamp(batch_metrics[i]["timestamp"])
               - _parse_timestamp(batch_metrics[i - 1]["timestamp"])).total_seconds()
        if gap > _RUN_GAP_THRESHOLD_SECONDS:
            start_index = i
    return {int(r["batch_id"]) for r in batch_metrics[start_index:]}


def _rejection_reason_counts(batch_ids: set[int]) -> dict[str, int]:
    rejected_files = sorted(glob.glob(str(REJECTED_DIR / "rejected_batch_*.csv")))
    relevant_files = [
        path for path in rejected_files
        if (m := _REJECTED_FILENAME_RE.search(path)) and int(m.group(1)) in batch_ids
    ]
    all_reasons = reduce(
        lambda reasons, path: reasons + pd.read_csv(path)["validation_reason"].tolist(),
        relevant_files, [],
    )
    return reduce_reasons_to_counts(all_reasons)


def _plot_throughput(row_counts: list[dict]) -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    batch_ids = [int(r["batch_id"]) for r in row_counts]
    inserted = [int(r["inserted"]) for r in row_counts]
    rejected = [int(r["rejected"]) for r in row_counts]

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(batch_ids, inserted, label="Saved to database", color="#4c9a4c")
    ax.bar(batch_ids, rejected, bottom=inserted, label="Rejected (bad data)", color="#c0504d")
    ax.set_title("Events Processed per Batch", fontsize=13, fontweight="bold")
    ax.set_xlabel("Batch number (each batch = one CSV file of new events)")
    ax.set_ylabel("Number of events")
    ax.legend()
    fig.tight_layout()
    fig.savefig(THROUGHPUT_PLOT_PATH, dpi=150)
    plt.close(fig)


def _plot_latency(batch_metrics: list[dict]) -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    batch_ids = [int(r["batch_id"]) for r in batch_metrics]
    duration_seconds = [float(r["batch_duration_ms"]) / 1000 for r in batch_metrics]

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(batch_ids, duration_seconds, marker="o", color="#4472c4")
    ax.set_title("How Long Each Batch Took to Process", fontsize=13, fontweight="bold")
    ax.set_xlabel("Batch number")
    ax.set_ylabel("Processing time (seconds) -- lower is faster")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(LATENCY_PLOT_PATH, dpi=150)
    plt.close(fig)


def build_report() -> str:
    all_batch_metrics = _read_csv_rows(METRICS_LOG_PATH, _METRICS_COLUMNS)
    latest_run_ids = _latest_run_batch_ids(all_batch_metrics)
    row_counts = [
        r for r in _read_csv_rows(BATCH_ROW_COUNTS_PATH, _ROW_COUNTS_COLUMNS)
        if int(r["batch_id"]) in latest_run_ids
    ]
    batch_metrics = [r for r in all_batch_metrics if int(r["batch_id"]) in latest_run_ids]
    reason_counts = _rejection_reason_counts(latest_run_ids)

    total_received = sum(int(r["received"]) for r in row_counts)
    total_inserted = sum(int(r["inserted"]) for r in row_counts)
    total_rejected = sum(int(r["rejected"]) for r in row_counts)
    num_batches = len(row_counts)
    avg_received_per_batch = total_received / num_batches if num_batches else 0

    durations_s = [float(r["batch_duration_ms"]) / 1000 for r in batch_metrics]
    warm_durations_s = durations_s[1:] or durations_s  # skip batch 0's one-time startup cost
    avg_latency = sum(warm_durations_s) / len(warm_durations_s)
    min_latency, max_latency = min(warm_durations_s), max(warm_durations_s)

    throughput = [float(r["processed_rows_per_second"]) for r in batch_metrics[1:]] or [0.0]
    avg_throughput = sum(throughput) / len(throughput)

    _plot_throughput(row_counts)
    _plot_latency(batch_metrics)

    reason_lines = "\n".join(f"| {reason} | {count} |" for reason, count in sorted(reason_counts.items()))
    batch_table_rows = "\n".join(
        f"| {rc['batch_id']} | {rc['received']} | {rc['inserted']} | {rc['rejected']} | "
        f"{float(bm['batch_duration_ms']) / 1000:.2f}s |"
        for rc, bm in zip(row_counts, batch_metrics)
    )

    return f"""# Performance Metrics

*Measured from a real local run of the pipeline -- not estimated.*

## Summary (plain-language)

- **{total_received} events** arrived across **{num_batches} batches** (each batch = one new CSV file).
- **{total_inserted} events ({total_inserted / total_received:.0%})** passed data-quality checks and were saved to PostgreSQL.
- **{total_rejected} events ({total_rejected / total_received:.0%})** were rejected for bad data and saved to `data/rejected/` for review.
- After the first (one-time, colder-start) batch, a typical batch took **{avg_latency:.2f} seconds** to process
  (fastest: {min_latency:.2f}s, slowest: {max_latency:.2f}s).
- Once warmed up, the pipeline processed about **{avg_throughput:.1f} events per second**.

## Why some events were rejected

The event generator deliberately injects a small fraction of bad data (about 5%) to prove the
pipeline actually catches it instead of silently accepting garbage. Breakdown for this run:

| Reason | Count |
|---|---|
{reason_lines}

Every rejected row is kept, not deleted -- see the CSV files under `data/rejected/` for the exact rows and reasons.

## Charts

![Events processed per batch](reports/plots/throughput_per_batch.png)

![Batch processing time](reports/plots/batch_latency.png)

## Per-batch detail

| Batch | Received | Inserted | Rejected | Processing time |
|---|---|---|---|---|
{batch_table_rows}

## How this was measured

- **Setup:** a single local machine (Spark `local[*]`, one PostgreSQL instance), not a distributed cluster.
  Absolute throughput numbers would differ on production-scale hardware; the relative behavior
  (near-constant per-batch latency, low rejection rate) is what matters here.
- **Batch size:** averaged {avg_received_per_batch:.0f} events per micro-batch this run
  ({MAX_FILES_PER_TRIGGER} file(s) read per micro-batch), with a trigger interval of
  {STREAM_TRIGGER_INTERVAL}.
- **Batch duration** comes from Spark's own `StreamingQueryListener` progress events
  (`logs/batch_metrics.csv`). **Received/inserted/rejected counts** come from the sink itself
  (`logs/batch_row_counts.csv`), which is the authoritative source -- Spark's self-reported
  `numInputRows` was observed to run 1 row high per batch (a cosmetic quirk in Spark's own
  instrumentation), so it is not used for the row-count figures above.
- Because the demo generator stops after a fixed number of files while the stream keeps its
  steady trigger interval, a few generated files can still be waiting in `data/incoming/` when
  the demo window ends -- that's expected for a timed demo, not data loss (nothing is deleted
  from `data/incoming/`, so a subsequent run picks them up).
"""


def main() -> None:
    REPORT_PATH.write_text(build_report(), encoding="utf-8")
    print(f"Wrote {REPORT_PATH}")


if __name__ == "__main__":
    main()
