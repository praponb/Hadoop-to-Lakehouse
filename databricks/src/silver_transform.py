"""Silver: cleanse, quarantine, deduplicate and MERGE one Bronze batch.

The transformation functions only use pyspark.sql, so tests/test_silver_transformations.py
runs them on a local SparkSession. Only main() touches Delta and Unity Catalog.

Idempotency for a re-run of the same --batch-date:
  * orders_quarantine: that batch_date's rows are replaced (replaceWhere), not appended.
  * orders_cleansed: MERGE only updates when source.updated_at > target.updated_at,
    so replaying a batch that's already applied changes nothing.
"""
from __future__ import annotations

import os
import sys

try:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
except NameError:  # __file__ is undefined in some Databricks launch modes
    sys.path.insert(0, os.getcwd())

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

TIMESTAMP_FORMATS = ("yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss")
VALID_STATUSES = ("PENDING", "PAID", "SHIPPED", "CANCELLED")
STATUS_ALIASES = {"CANCELED": "CANCELLED"}
CUSTOMER_ID_PATTERN = r"^C\d{6}$"

SOURCE_COLUMNS = [
    "order_id",
    "customer_id",
    "product_sku",
    "quantity",
    "unit_price",
    "order_status",
    "order_timestamp",
    "updated_at",
    "discount_code",
]
SILVER_COLUMNS = [
    "order_id",
    "customer_id",
    "product_sku",
    "quantity",
    "unit_price",
    "order_status",
    "order_timestamp",
    "updated_at",
    "discount_code",
    "_batch_date",
]
QUARANTINE_COLUMNS = SOURCE_COLUMNS + ["error_reason", "_source_file", "batch_date"]


def _clean_str(c: Column) -> Column:
    """Trimmed string, with empty strings as NULL."""
    trimmed = F.trim(c.cast("string"))
    return F.when(trimmed == "", None).otherwise(trimmed)


def parse_timestamp(c: Column) -> Column:
    """Parse either legacy timestamp format. try_to_timestamp stays NULL-safe under ANSI mode."""
    s = _clean_str(c)
    return F.coalesce(*(F.try_to_timestamp(s, F.lit(fmt)) for fmt in TIMESTAMP_FORMATS))


def standardize_status(c: Column) -> Column:
    s = F.upper(_clean_str(c))
    for alias, canonical in STATUS_ALIASES.items():
        s = F.when(s == alias, canonical).otherwise(s)
    return F.when(s.isin(*VALID_STATUSES), s)


def with_source_columns(df: DataFrame) -> DataFrame:
    """Add any source column missing from this batch (discount_code before day 2) as NULL."""
    for name in SOURCE_COLUMNS:
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast("string"))
    for name, dtype in (("_source_file", "string"), ("batch_date", "date")):
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast(dtype))
    return df


def standardize(df: DataFrame) -> DataFrame:
    """Type enforcement and value standardisation. Raw values are kept as raw_<col> for quarantine."""
    df = with_source_columns(df)
    raw = [F.col(c).cast("string").alias(f"raw_{c}") for c in SOURCE_COLUMNS]
    return df.select(
        *raw,
        _clean_str(F.col("order_id")).alias("order_id"),
        _clean_str(F.col("customer_id")).alias("customer_id"),
        F.upper(_clean_str(F.col("product_sku"))).alias("product_sku"),
        F.expr("try_cast(trim(cast(quantity AS STRING)) AS INT)").alias("quantity"),
        F.expr("try_cast(trim(cast(unit_price AS STRING)) AS DECIMAL(10,2))").alias("unit_price"),
        standardize_status(F.col("order_status")).alias("order_status"),
        parse_timestamp(F.col("order_timestamp")).alias("order_timestamp"),
        parse_timestamp(F.col("updated_at")).alias("updated_at"),
        _clean_str(F.col("discount_code")).alias("discount_code"),
        F.col("_source_file").cast("string").alias("_source_file"),
        F.col("batch_date").cast("date").alias("batch_date"),
    )


def error_reason() -> Column:
    """'; '-joined list of failed data-quality rules, or NULL if the row is valid."""
    rules = [
        (F.col("order_id").isNull(), "missing_order_id"),
        (F.col("customer_id").isNull(), "missing_customer_id"),
        (F.col("customer_id").isNotNull() & ~F.col("customer_id").rlike(CUSTOMER_ID_PATTERN), "malformed_customer_id"),
        (F.col("quantity").isNull(), "invalid_quantity"),
        (F.col("quantity") <= 0, "non_positive_quantity"),
        (F.col("unit_price").isNull(), "invalid_unit_price"),
        (F.col("unit_price") < 0, "negative_unit_price"),
        (F.col("order_status").isNull(), "unknown_order_status"),
        (F.col("order_timestamp").isNull(), "unparseable_order_timestamp"),
        (F.col("updated_at").isNull(), "unparseable_updated_at"),
    ]
    reasons = F.concat_ws("; ", *(F.when(cond, F.lit(label)) for cond, label in rules))
    return F.when(reasons == "", None).otherwise(reasons)


def cleanse_and_split_df(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split a Bronze batch into (valid, quarantine). Valid rows are typed but not yet deduplicated."""
    checked = standardize(df).withColumn("error_reason", error_reason())
    valid = checked.where(F.col("error_reason").isNull()).select(
        *SILVER_COLUMNS[:-1], F.col("batch_date").alias("_batch_date")
    )
    quarantine = checked.where(F.col("error_reason").isNotNull()).select(
        *(F.col(f"raw_{c}").alias(c) for c in SOURCE_COLUMNS), "error_reason", "_source_file", "batch_date"
    )
    return valid, quarantine


def dedupe_latest(df: DataFrame) -> DataFrame:
    """Keep one row per order_id: the latest updated_at (ties broken deterministically)."""
    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc(), F.col("order_status").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def main() -> None:
    from delta.tables import DeltaTable

    from common.spark_session import get_spark, parse_args, require

    args = parse_args()
    require(args, "batch_date")
    spark = get_spark("silver_transform")
    ds = args.batch_date.isoformat()
    bronze_table = f"{args.catalog}.bronze.orders_raw"
    silver_table = f"{args.catalog}.silver.orders_cleansed"
    quarantine_table = f"{args.catalog}.silver.orders_quarantine"

    bronze = spark.table(bronze_table).where(F.col("batch_date") == F.lit(ds).cast("date"))
    bronze_count = bronze.count()
    if bronze_count == 0:
        raise RuntimeError(f"No rows in {bronze_table} for batch_date={ds}; did the Bronze task run?")

    valid, quarantine = cleanse_and_split_df(bronze)
    latest = dedupe_latest(valid)  # no .cache(): serverless compute doesn't support it

    quarantine.write.format("delta").mode("overwrite").option("replaceWhere", f"batch_date = DATE'{ds}'").saveAsTable(quarantine_table)

    (
        DeltaTable.forName(spark, silver_table)
        .alias("target")
        .merge(latest.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdateAll(condition="source.updated_at > target.updated_at")
        .whenNotMatchedInsertAll()
        .execute()
    )

    quarantined = spark.table(quarantine_table).where(F.col("batch_date") == F.lit(ds).cast("date")).count()
    print(f"batch_date={ds}: bronze={bronze_count} valid_unique={latest.count()} quarantined={quarantined}")


if __name__ == "__main__":
    main()
