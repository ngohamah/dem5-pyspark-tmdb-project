"""Configuration values for the streaming pipeline.

Centralizing paths, generator settings, and connection info here means the
rest of the pipeline never hard-codes a literal path or constant inline.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

DATA_DIR = BASE_DIR / "data"
INCOMING_DIR = DATA_DIR / "incoming"
ARCHIVE_DIR = DATA_DIR / "archive"
REJECTED_DIR = DATA_DIR / "rejected"

CHECKPOINT_DIR = BASE_DIR / "checkpoints" / "events"
LOG_DIR = BASE_DIR / "logs"
METRICS_LOG_PATH = LOG_DIR / "batch_metrics.csv"
# Ground-truth row counts per micro-batch (received/inserted/rejected), written
# by the sink itself. Spark's own progress metrics (METRICS_LOG_PATH) report
# numInputRows with a small, cosmetic off-by-one vs. what the sink actually
# saw -- this file is what performance_metrics.md is built from.
BATCH_ROW_COUNTS_PATH = LOG_DIR / "batch_row_counts.csv"

REPORTS_DIR = BASE_DIR / "reports"
PLOTS_DIR = REPORTS_DIR / "plots"

# --- Event generation -------------------------------------------------------

EVENT_TYPES = ("view", "add_to_cart", "purchase")
EVENT_TYPE_WEIGHTS = (0.60, 0.25, 0.15)

# Static product catalog -- a real system would look this up from a product
# service, but a fixed list keeps the generator deterministic and dependency-free.
PRODUCT_CATALOG = [
    {"product_id": "P001", "product_name": "Wireless Mouse", "category": "Electronics", "price": 19.99},
    {"product_id": "P002", "product_name": "Mechanical Keyboard", "category": "Electronics", "price": 59.99},
    {"product_id": "P003", "product_name": "USB-C Hub", "category": "Electronics", "price": 24.50},
    {"product_id": "P004", "product_name": "Noise-Cancelling Headphones", "category": "Electronics", "price": 129.00},
    {"product_id": "P005", "product_name": "Python Cookbook", "category": "Books", "price": 34.95},
    {"product_id": "P006", "product_name": "Data Engineering Handbook", "category": "Books", "price": 42.00},
    {"product_id": "P007", "product_name": "Ceramic Coffee Mug", "category": "Home", "price": 12.99},
    {"product_id": "P008", "product_name": "Standing Desk Mat", "category": "Home", "price": 45.00},
    {"product_id": "P009", "product_name": "Cotton T-Shirt", "category": "Clothing", "price": 15.00},
    {"product_id": "P010", "product_name": "Running Shoes", "category": "Clothing", "price": 89.99},
    {"product_id": "P011", "product_name": "Building Blocks Set", "category": "Toys", "price": 27.50},
    {"product_id": "P012", "product_name": "Remote Control Car", "category": "Toys", "price": 39.99},
]

NUM_USERS = 500
DEFAULT_EVENTS_PER_FILE = 50
DEFAULT_FILE_INTERVAL_SECONDS = 5.0

# Fraction of generated events deliberately corrupted (bad price, missing
# product id, unknown event type, ...) so the Spark cleaning stage -- and its
# tests -- have real invalid rows to catch instead of only clean happy-path data.
MALFORMED_EVENT_RATE = 0.05

CSV_COLUMNS = [
    "event_id", "event_time", "user_id", "session_id",
    "event_type", "product_id", "product_name", "category", "price", "quantity",
]

# --- Spark -------------------------------------------------------------------

SPARK_APP_NAME = "ecommerce-event-streaming"
SPARK_MASTER = "local[*]"
STREAM_TRIGGER_INTERVAL = "5 seconds"
MAX_FILES_PER_TRIGGER = 1
CORRUPT_RECORD_COLUMN = "_corrupt_record"

# --- PostgreSQL ---------------------------------------------------------------

# Set these in a local .env file (see .env.example) or the shell environment --
# never hard-code a real password here, since this file is tracked in git.
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "streaming_events")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "streaming_app")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")
POSTGRES_TABLE = "events"
