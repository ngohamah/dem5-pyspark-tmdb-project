# Performance Metrics

*Measured from a real local run of the pipeline -- not estimated.*

## Summary (plain-language)

- **34200 events** arrived across **19 batches** (each batch = one new CSV file).
- **32517 events (95%)** passed data-quality checks and were saved to PostgreSQL.
- **1683 events (5%)** were rejected for bad data and saved to `data/rejected/` for review.
- After the first (one-time, colder-start) batch, a typical batch took **0.69 seconds** to process
  (fastest: 0.63s, slowest: 0.79s).
- Once warmed up, the pipeline processed about **2609.4 events per second**.

## Why some events were rejected

The event generator deliberately injects a small fraction of bad data (about 5%) to prove the
pipeline actually catches it instead of silently accepting garbage. Breakdown for this run:

| Reason | Count |
|---|---|
| invalid_event_time | 314 |
| invalid_event_type | 330 |
| invalid_price | 353 |
| invalid_quantity | 346 |
| missing_product_id | 340 |

Every rejected row is kept, not deleted -- see the CSV files under `data/rejected/` for the exact rows and reasons.

## Charts

![Events processed per batch](reports/plots/throughput_per_batch.png)

![Batch processing time](reports/plots/batch_latency.png)

## Per-batch detail

| Batch | Received | Inserted | Rejected | Processing time |
|---|---|---|---|---|
| 0 | 1800 | 1706 | 94 | 2.83s |
| 1 | 1800 | 1704 | 96 | 0.79s |
| 2 | 1800 | 1713 | 87 | 0.72s |
| 3 | 1800 | 1705 | 95 | 0.68s |
| 4 | 1800 | 1710 | 90 | 0.67s |
| 5 | 1800 | 1711 | 89 | 0.67s |
| 6 | 1800 | 1708 | 92 | 0.76s |
| 7 | 1800 | 1734 | 66 | 0.74s |
| 8 | 1800 | 1713 | 87 | 0.70s |
| 9 | 1800 | 1711 | 89 | 0.75s |
| 10 | 1800 | 1720 | 80 | 0.66s |
| 11 | 1800 | 1724 | 76 | 0.63s |
| 12 | 1800 | 1706 | 94 | 0.64s |
| 13 | 1800 | 1710 | 90 | 0.66s |
| 14 | 1800 | 1714 | 86 | 0.73s |
| 15 | 1800 | 1717 | 83 | 0.64s |
| 16 | 1800 | 1701 | 99 | 0.68s |
| 17 | 1800 | 1701 | 99 | 0.64s |
| 18 | 1800 | 1709 | 91 | 0.72s |

## How this was measured

- **Setup:** a single local machine (Spark `local[*]`, one PostgreSQL instance), not a distributed cluster.
  Absolute throughput numbers would differ on production-scale hardware; the relative behavior
  (near-constant per-batch latency, low rejection rate) is what matters here.
- **Batch size:** averaged 1800 events per micro-batch this run
  (1 file(s) read per micro-batch), with a trigger interval of
  5 seconds.
- **Batch duration** comes from Spark's own `StreamingQueryListener` progress events
  (`logs/batch_metrics.csv`). **Received/inserted/rejected counts** come from the sink itself
  (`logs/batch_row_counts.csv`), which is the authoritative source -- Spark's self-reported
  `numInputRows` was observed to run 1 row high per batch (a cosmetic quirk in Spark's own
  instrumentation), so it is not used for the row-count figures above.
- Because the demo generator stops after a fixed number of files while the stream keeps its
  steady trigger interval, a few generated files can still be waiting in `data/incoming/` when
  the demo window ends -- that's expected for a timed demo, not data loss (nothing is deleted
  from `data/incoming/`, so a subsequent run picks them up).
