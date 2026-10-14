"""Silver transformation logic on a local SparkSession (no Delta or Databricks needed)."""
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver_transform import cleanse_and_split_df, dedupe_latest, SILVER_COLUMNS, QUARANTINE_COLUMNS

BATCH = date(2026, 10, 14)
BRONZE_SCHEMA = (
    "order_id string, customer_id string, product_sku string, quantity string, unit_price string, "
    "order_status string, order_timestamp string, updated_at string, _source_file string, batch_date date"
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test_silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def row(order_id="ORD-1", customer_id="C000001", sku="SKU-1001", qty="2", price="10.50", status="PAID",
        ts="2026-10-14 08:00:00", updated="2026-10-14 08:05:00"):
    return (order_id, customer_id, sku, qty, price, status, ts, updated, "gs://landing/erp/orders/ingest_date=2026-10-14/orders_part_0.csv", BATCH)


def bronze(spark, *rows):
    return spark.createDataFrame(list(rows), BRONZE_SCHEMA)


def split(spark, *rows):
    valid, quarantine = cleanse_and_split_df(bronze(spark, *rows))
    return valid.collect(), quarantine.collect()


# --- timestamp parsing ---------------------------------------------------------

def test_both_timestamp_formats_parse_to_utc(spark):
    valid, _ = split(spark, row("A", ts="2026-10-14 08:00:00", updated="2026/10/14 09:30:15"))
    assert valid[0].order_timestamp == datetime(2026, 10, 14, 8, 0, 0)
    assert valid[0].updated_at == datetime(2026, 10, 14, 9, 30, 15)


def test_unparseable_timestamp_is_quarantined(spark):
    valid, quarantine = split(spark, row("A", ts="14-10-2026 08:00"))
    assert valid == []
    assert quarantine[0].error_reason == "unparseable_order_timestamp"


# --- type enforcement and standardisation ---------------------------------------

def test_types_and_standardisation(spark):
    valid, _ = split(spark, row("A", sku="  sku-1001 ", qty="3", price="19.999", status=" canceled "))
    r = valid[0]
    assert r.product_sku == "SKU-1001"
    assert r.quantity == 3
    assert r.unit_price == Decimal("20.00")
    assert r.order_status == "CANCELLED"
    assert r._batch_date == BATCH


def test_output_schemas(spark):
    valid, quarantine = cleanse_and_split_df(bronze(spark, row()))
    assert valid.columns == SILVER_COLUMNS
    assert quarantine.columns == QUARANTINE_COLUMNS
    assert dict(valid.dtypes)["unit_price"] == "decimal(10,2)"
    assert dict(valid.dtypes)["quantity"] == "int"


def test_missing_discount_code_column_is_added_as_null(spark):
    valid, _ = split(spark, row())
    assert valid[0].discount_code is None


def test_discount_code_is_kept_when_present(spark):
    df = bronze(spark, row(), row("B")).withColumn("discount_code", F.lit(" VIP15 "))
    valid, _ = cleanse_and_split_df(df)
    assert {r.discount_code for r in valid.collect()} == {"VIP15"}


# --- quarantine splitting ---------------------------------------------------------

@pytest.mark.parametrize(
    "kwargs, reason",
    [
        ({"order_id": None}, "missing_order_id"),
        ({"customer_id": ""}, "missing_customer_id"),
        ({"customer_id": None}, "missing_customer_id"),
        ({"customer_id": "N/A"}, "malformed_customer_id"),
        ({"qty": "0"}, "non_positive_quantity"),
        ({"qty": "-3"}, "non_positive_quantity"),
        ({"qty": "two"}, "invalid_quantity"),
        ({"price": "-1.00"}, "negative_unit_price"),
        ({"price": "abc"}, "invalid_unit_price"),
        ({"status": "SHIPPING"}, "unknown_order_status"),
    ],
)
def test_invalid_rows_are_quarantined_with_reason(spark, kwargs, reason):
    valid, quarantine = split(spark, row(**kwargs))
    assert valid == []
    assert quarantine[0].error_reason == reason
    assert quarantine[0].batch_date == BATCH


def test_multiple_failures_are_all_reported(spark):
    _, quarantine = split(spark, row(customer_id="", qty="0"))
    assert quarantine[0].error_reason == "missing_customer_id; non_positive_quantity"


def test_quarantine_keeps_raw_values(spark):
    _, quarantine = split(spark, row(customer_id="N/A", sku="  sku-1001 ", qty="-1"))
    assert quarantine[0].product_sku == "  sku-1001 "
    assert quarantine[0].quantity == "-1"


def test_split_is_a_partition_of_the_input(spark):
    rows = [row("A"), row("B", customer_id=""), row("C", qty="0"), row("D"), row("E", price="-2")]
    valid, quarantine = split(spark, *rows)
    assert len(valid) + len(quarantine) == len(rows)
    assert {r.order_id for r in valid} == {"A", "D"}


# --- deduplication ----------------------------------------------------------------

def test_dedupe_keeps_latest_updated_at(spark):
    valid, _ = cleanse_and_split_df(bronze(
        spark,
        row("A", status="PENDING", updated="2026-10-14 08:00:00"),
        row("A", status="CANCELLED", updated="2026/10/14 12:00:00"),
        row("A", status="SHIPPED", updated="2026-10-14 10:00:00"),
        row("B", status="PAID"),
    ))
    latest = {r.order_id: r for r in dedupe_latest(valid).collect()}
    assert set(latest) == {"A", "B"}
    assert latest["A"].order_status == "CANCELLED"
    assert latest["A"].updated_at == datetime(2026, 10, 14, 12, 0, 0)


def test_dedupe_collapses_exact_duplicates(spark):
    valid, _ = cleanse_and_split_df(bronze(spark, row("A"), row("A"), row("A")))
    assert dedupe_latest(valid).count() == 1


def test_dedupe_preserves_columns(spark):
    valid, _ = cleanse_and_split_df(bronze(spark, row("A")))
    assert dedupe_latest(valid).columns == SILVER_COLUMNS


# --- end to end on generator output -------------------------------------------------

def test_generator_batch_end_to_end(spark):
    from generate_legacy_data import generate_rows

    columns, rows = generate_rows(BATCH, 2000, evolve_schema=True, seed=11)
    df = spark.createDataFrame([[r[c] for c in columns] for r in rows], columns)
    df = df.withColumn("_source_file", F.lit("f.csv"))
    df = df.withColumn("batch_date", F.lit(BATCH))

    valid, quarantine = cleanse_and_split_df(df)
    latest = dedupe_latest(valid)
    n_valid, n_quarantine, n_latest = valid.count(), quarantine.count(), latest.count()

    assert n_valid + n_quarantine == 2000
    assert 0.02 <= n_quarantine / 2000 <= 0.06
    assert n_latest < n_valid  # the generator's duplicate order_ids were collapsed
    assert latest.select("order_id").distinct().count() == n_latest
