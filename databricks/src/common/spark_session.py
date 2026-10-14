"""Shared Spark session and job-parameter parsing for the medallion jobs.

Jobs run as `spark_python_task`s (from the Asset Bundle or Airflow's
DatabricksSubmitRunOperator), so parameters arrive as CLI arguments.
Environment variables are the fallback for local runs.
"""
from __future__ import annotations

import argparse
import os
from datetime import date

from pyspark.sql import SparkSession


def get_spark(app_name: str = "hadoop_to_lakehouse") -> SparkSession:
    """Return the active session (Databricks) or build one; timestamps are handled in UTC.

    The Databricks session is reused as-is: on serverless (Spark Connect), static
    settings such as the app name can't be changed on a running session.
    """
    spark = SparkSession.getActiveSession() or SparkSession.builder.appName(app_name).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-date", type=date.fromisoformat, default=None, help="Airflow ds, YYYY-MM-DD")
    parser.add_argument("--catalog", default=os.environ.get("DATABRICKS_CATALOG", "legacy_migration_cat"))
    parser.add_argument("--landing-path", default=os.environ.get("GCS_LANDING_BUCKET"), help="gs://landing bucket")
    parser.add_argument("--lakehouse-path", default=os.environ.get("GCS_LAKEHOUSE_BUCKET"), help="gs://lakehouse bucket")
    # Databricks can append its own arguments; ignore anything we don't know.
    args, _ = parser.parse_known_args(argv)
    for name in ("landing_path", "lakehouse_path"):
        if getattr(args, name):
            setattr(args, name, getattr(args, name).rstrip("/"))
    return args


def require(args: argparse.Namespace, *names: str) -> None:
    missing = [f"--{n.replace('_', '-')}" for n in names if not getattr(args, n)]
    if missing:
        raise SystemExit(f"Missing required job parameter(s): {', '.join(missing)}")
