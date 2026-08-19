# Project Overview: Real-Time E-Commerce Event Pipeline

## What this project does

This project simulates a small e-commerce website and tracks what shoppers
do on it -- viewing products, adding them to a cart, and buying them -- in
close to real time. Instead of waiting for a nightly batch job, each event
flows through the system within seconds of being "generated," gets checked
for obvious data-quality problems, and lands in a database table ready to
query.

It's a demonstration pipeline: the "shoppers" are simulated by a script
rather than a real website, but the ingestion, cleaning, and storage
components work exactly as they would with real traffic.

## Why it's built this way

A traditional batch report answers "what happened yesterday." This
pipeline is built to answer "what is happening right now" -- the kind of
question needed to catch a broken checkout flow, a fraud spike, or a sudden
change in what's selling, while it's still actionable.

## The three components

See [system_architecture.png](system_architecture.png) for the full data-flow diagram. In plain terms:

1. **Event Generator** (`data_generator.py`) -- simulates shopper activity.
   Every few seconds it writes a new file of fake events (a mix of views,
   cart-adds, and purchases) to `data/incoming/`. A small fraction of
   events are deliberately made invalid (a negative price, a missing
   product, a garbled timestamp) to prove the next stage actually catches
   bad data rather than just passing everything through.

2. **Spark Structured Streaming job** (`spark_streaming_to_postgres.py`) --
   watches that folder. As soon as a new file appears, it:
   - Reads the raw text and converts it to proper types (numbers, dates).
   - Flags any row that fails a data-quality rule (bad price, unknown
     event type, missing required field, etc.) and records *why*.
   - Sends the good rows to the database and saves the bad rows to
     `data/rejected/` for someone to review later -- nothing is silently
     thrown away.
   - Logs how many rows it processed and how fast, for every batch.

3. **PostgreSQL database** -- stores every valid event in a single
   `events` table (`postgres_setup.sql` creates it), ready for a
   dashboard, an analyst's SQL query, or a downstream report.

## What happens to bad data

Every event that fails validation is kept, not deleted -- it's written
to a CSV file under `data/rejected/` along with the specific reason it
failed (e.g. `invalid_price`, `missing_product_id`), and a warning is
logged. This means nothing disappears without a trace: anyone can open
those files and see exactly what was rejected and why.

## Related documents

- [user_guide.md](user_guide.md) -- how to set up and run the pipeline yourself
- [test_cases.md](test_cases.md) -- the manual test plan and results
- [performance_metrics.md](performance_metrics.md) -- throughput/latency from a real run
