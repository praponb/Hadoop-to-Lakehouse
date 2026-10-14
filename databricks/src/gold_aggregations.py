"""Gold: rebuild daily_sales_summary and customer_ltv from Silver.

Both tables are recomputed in full and written with INSERT OVERWRITE into the
liquid-clustered tables created by setup_uc.sql, so every run, re-runs
included, leaves exactly one row per key. The tables are UC external tables at
{lakehouse}/gold/<table>, where BigQuery BigLake reads them
(bigquery/create_biglake_tables.sql).
"""
from __future__ import annotations

import os
import sys

try:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
except NameError:  # __file__ is undefined in some Databricks launch modes
    sys.path.insert(0, os.getcwd())

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

DAILY_SALES_COLUMNS = ["order_date", "product_sku", "gross_revenue", "net_revenue", "total_orders", "cancellation_rate"]
CUSTOMER_LTV_COLUMNS = ["customer_id", "first_order_date", "last_order_date", "lifetime_orders", "lifetime_spend"]


def _amount() -> Column:
    return (F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)")


def _is_cancelled() -> Column:
    return F.col("order_status") == "CANCELLED"


def build_daily_sales_summary(silver: DataFrame) -> DataFrame:
    """Per order_date and SKU. net_revenue and the cancellation rate treat CANCELLED orders as lost."""
    return (
        silver.withColumn("order_date", F.to_date("order_timestamp"))
        .groupBy("order_date", "product_sku")
        .agg(
            F.sum(_amount()).cast("decimal(18,2)").alias("gross_revenue"),
            F.sum(F.when(~_is_cancelled(), _amount()).otherwise(F.lit(0))).cast("decimal(18,2)").alias("net_revenue"),
            F.count("*").alias("total_orders"),
            F.round(F.avg(F.when(_is_cancelled(), 1.0).otherwise(0.0)), 4).cast("decimal(5,4)").alias("cancellation_rate"),
        )
        .select(*DAILY_SALES_COLUMNS)
    )


def build_customer_ltv(silver: DataFrame) -> DataFrame:
    """Per customer. Cancelled orders count toward order dates and counts but not spend."""
    return (
        silver.withColumn("order_date", F.to_date("order_timestamp"))
        .groupBy("customer_id")
        .agg(
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.count("*").alias("lifetime_orders"),
            F.sum(F.when(~_is_cancelled(), _amount()).otherwise(F.lit(0))).cast("decimal(18,2)").alias("lifetime_spend"),
        )
        .select(*CUSTOMER_LTV_COLUMNS)
    )


def overwrite_table(df: DataFrame, table: str, view: str) -> None:
    """INSERT OVERWRITE keeps the table's clustering keys and properties, unlike a CTAS replace."""
    df.createOrReplaceTempView(view)
    df.sparkSession.sql(f"INSERT OVERWRITE {table} SELECT * FROM {view}")


def main() -> None:
    from common.spark_session import get_spark, parse_args

    args = parse_args()
    spark = get_spark("gold_aggregations")
    silver = spark.table(f"{args.catalog}.silver.orders_cleansed")

    targets = {
        f"{args.catalog}.gold.daily_sales_summary": build_daily_sales_summary(silver),
        f"{args.catalog}.gold.customer_ltv": build_customer_ltv(silver),
    }
    for table, df in targets.items():
        overwrite_table(df, table, view=f"_{table.rsplit('.', 1)[1]}_staging")
        print(f"{table}: {spark.table(table).count()} rows")


if __name__ == "__main__":
    main()
