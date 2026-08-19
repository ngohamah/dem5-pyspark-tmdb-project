"""SparkSession creation for the streaming pipeline."""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from .config import SPARK_APP_NAME, SPARK_MASTER


def get_spark_session(app_name: str = SPARK_APP_NAME, master: str = SPARK_MASTER) -> SparkSession:
    """Create (or reuse) a local SparkSession.

    Pins the worker Python to the interpreter currently running so driver
    and executor Python minor versions always match, regardless of what
    ``python3`` resolves to on the caller's PATH. Session timezone is fixed
    to UTC so timestamp parsing/display doesn't depend on the host machine's
    local timezone.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
