"""Bronze: Auto Loader (cloudFiles) from the GCS landing zone into {catalog}.bronze.orders_raw.

Runs with trigger(availableNow=True): it ingests every file not yet recorded in
the checkpoint and stops. The checkpoint makes re-runs idempotent, since a file
already ingested is never read again.

With schemaEvolutionMode=addNewColumns, a new column (day 2's discount_code)
stops the stream with UnknownFieldException after recording the new schema.
Databricks fails the whole task when any stream stops with an error, even if
the script catches it, so the restart is left to a task retry: max_retries on
bronze_ingest in databricks.yml, and the Airflow task's retries. The retried
run reads the evolved schema and carries on from the checkpoint.
"""
from __future__ import annotations

import os
import sys

try:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
except NameError:  # __file__ is undefined in some Databricks launch modes
    sys.path.insert(0, os.getcwd())

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.spark_session import get_spark, parse_args, require

PARTITION_DATE_PATTERN = r"ingest_date=(\d{4}-\d{2}-\d{2})"


def read_landing(spark: SparkSession, source_path: str, schema_location: str) -> DataFrame:
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("header", "true")
        .load(source_path)
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("batch_date", F.to_date(F.regexp_extract(F.col("_metadata.file_path"), PARTITION_DATE_PATTERN, 1)))
    )


def run_once(spark: SparkSession, source_path: str, lakehouse_path: str, target_table: str) -> None:
    query = (
        read_landing(spark, source_path, f"{lakehouse_path}/_schemas/bronze_orders")
        .writeStream.format("delta")
        .option("checkpointLocation", f"{lakehouse_path}/_checkpoints/bronze_orders")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()


def main() -> None:
    args = parse_args()
    require(args, "landing_path", "lakehouse_path")
    spark = get_spark("bronze_autoloader")
    source_path = f"{args.landing_path}/erp/orders/"
    target_table = f"{args.catalog}.bronze.orders_raw"

    run_once(spark, source_path, args.lakehouse_path, target_table)

    counts = spark.table(target_table).groupBy("batch_date").count().orderBy("batch_date").collect()
    print(f"{target_table} rows by batch_date: " + ", ".join(f"{r.batch_date}={r['count']}" for r in counts))


if __name__ == "__main__":
    main()
