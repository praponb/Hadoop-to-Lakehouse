"""Gold aggregation logic on a local SparkSession."""
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from gold_aggregations import CUSTOMER_LTV_COLUMNS, DAILY_SALES_COLUMNS, build_customer_ltv, build_daily_sales_summary

SILVER_SCHEMA = (
    "order_id string, customer_id string, product_sku string, quantity int, unit_price decimal(10,2), "
    "order_status string, order_timestamp timestamp, updated_at timestamp, discount_code string, _batch_date date"
)


@pytest.fixture(scope="module")
def silver():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test_gold")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    d1, d2 = datetime(2026, 10, 14, 9), datetime(2026, 10, 15, 9)
    rows = [
        ("O1", "C000001", "SKU-1", 2, Decimal("10.00"), "PAID", d1, d1, None, date(2026, 10, 14)),
        ("O2", "C000001", "SKU-1", 1, Decimal("5.00"), "CANCELLED", d1, d1, None, date(2026, 10, 14)),
        ("O3", "C000002", "SKU-2", 3, Decimal("1.50"), "SHIPPED", d1, d1, None, date(2026, 10, 14)),
        ("O4", "C000001", "SKU-1", 4, Decimal("2.25"), "PENDING", d2, d2, "VIP15", date(2026, 10, 15)),
    ]
    return spark.createDataFrame(rows, SILVER_SCHEMA)


def test_daily_sales_summary(silver):
    out = {(r.order_date, r.product_sku): r for r in build_daily_sales_summary(silver).collect()}
    sku1_day1 = out[(date(2026, 10, 14), "SKU-1")]
    assert sku1_day1.gross_revenue == Decimal("25.00")
    assert sku1_day1.net_revenue == Decimal("20.00")
    assert sku1_day1.total_orders == 2
    assert sku1_day1.cancellation_rate == Decimal("0.5000")
    assert out[(date(2026, 10, 14), "SKU-2")].cancellation_rate == Decimal("0.0000")
    assert out[(date(2026, 10, 15), "SKU-1")].net_revenue == Decimal("9.00")
    assert len(out) == 3


def test_customer_ltv(silver):
    out = {r.customer_id: r for r in build_customer_ltv(silver).collect()}
    c1 = out["C000001"]
    assert (c1.first_order_date, c1.last_order_date) == (date(2026, 10, 14), date(2026, 10, 15))
    assert c1.lifetime_orders == 3
    assert c1.lifetime_spend == Decimal("29.00")  # 20.00 + 9.00, cancelled order excluded
    assert out["C000002"].lifetime_spend == Decimal("4.50")


def test_column_order_matches_table_ddl(silver):
    # INSERT OVERWRITE ... SELECT * is positional, so the order must match setup_uc.sql.
    assert build_daily_sales_summary(silver).columns == DAILY_SALES_COLUMNS
    assert build_customer_ltv(silver).columns == CUSTOMER_LTV_COLUMNS
