import csv
import re
from collections import Counter
from datetime import date

import pytest

from generate_legacy_data import (
    BASE_COLUMNS,
    EVOLVED_COLUMNS,
    generate_rows,
    write_partition,
)

BATCH_DATE = date(2026, 10, 14)
ROWS = 10_000
VALID_CUSTOMER = re.compile(r"^C\d{6}$")


@pytest.fixture(scope="module")
def batch():
    return generate_rows(BATCH_DATE, ROWS, seed=42)


def test_row_count_is_exact(batch):
    _, rows = batch
    assert len(rows) == ROWS


def test_duplicate_order_ids_injected(batch):
    _, rows = batch
    counts = Counter(r["order_id"] for r in rows if r["order_id"].startswith(f"ORD-{BATCH_DATE:%Y%m%d}"))
    extra_copies = sum(c - 1 for c in counts.values() if c > 1)
    assert extra_copies == pytest.approx(ROWS * 0.05, abs=1)


def test_duplicates_include_late_status_updates(batch):
    _, rows = batch
    by_id = {}
    for r in rows:
        by_id.setdefault(r["order_id"], set()).add(r["order_status"].strip().upper())
    assert any(len(statuses) > 1 for statuses in by_id.values())


def test_bad_customer_id_rate(batch):
    _, rows = batch
    bad = [r for r in rows if not VALID_CUSTOMER.match(r["customer_id"])]
    nulls = [r for r in bad if r["customer_id"] == ""]
    assert len(bad) / ROWS == pytest.approx(0.02, abs=0.001)
    assert len(nulls) > len(bad) / 2  # mostly NULLs, some malformed


def test_mixed_timestamp_formats(batch):
    _, rows = batch
    dashed = sum(1 for r in rows if re.match(r"^\d{4}-\d{2}-\d{2} ", r["order_timestamp"]))
    slashed = sum(1 for r in rows if re.match(r"^\d{4}/\d{2}/\d{2} ", r["order_timestamp"]))
    assert dashed + slashed == ROWS
    assert dashed > ROWS * 0.3 and slashed > ROWS * 0.3


def test_previous_day_late_updates(batch):
    _, rows = batch
    assert any(r["order_id"].startswith("ORD-20261013-") for r in rows)


def test_update_rows_keep_order_attributes(batch):
    _, rows = batch
    by_id = {}
    for r in rows:
        by_id.setdefault(r["order_id"], []).append(r)
    for versions in (v for v in by_id.values() if len(v) > 1):
        skus = {v["product_sku"].strip().upper() for v in versions}
        assert len(skus) == 1


def test_no_discount_code_by_default(batch):
    columns, rows = batch
    assert columns == BASE_COLUMNS
    assert all("discount_code" not in r for r in rows)


def test_evolve_schema_adds_discount_code():
    columns, rows = generate_rows(BATCH_DATE, 500, evolve_schema=True, seed=1)
    assert columns == EVOLVED_COLUMNS
    assert columns[-1] == "discount_code"
    assert any(r["discount_code"] for r in rows)
    assert any(r["discount_code"] == "" for r in rows)


def test_generation_is_deterministic_per_seed():
    assert generate_rows(BATCH_DATE, 200, seed=7) == generate_rows(BATCH_DATE, 200, seed=7)


def test_write_partition_layout(tmp_path):
    columns, rows = generate_rows(BATCH_DATE, 12_000, evolve_schema=True, seed=3)
    paths = write_partition(tmp_path, BATCH_DATE, columns, rows)
    assert [p.name for p in paths] == ["orders_part_0.csv", "orders_part_1.csv", "orders_part_2.csv"]
    assert all(p.parent.name == "ingest_date=2026-10-14" for p in paths)

    read_back = []
    for p in paths:
        with p.open() as fh:
            reader = csv.DictReader(fh)
            assert reader.fieldnames == EVOLVED_COLUMNS
            read_back.extend(reader)
    assert len(read_back) == 12_000


def test_write_partition_replaces_stale_parts(tmp_path):
    columns, rows = generate_rows(BATCH_DATE, 100, seed=3)
    write_partition(tmp_path, BATCH_DATE, columns, rows, parts=3)
    paths = write_partition(tmp_path, BATCH_DATE, columns, rows, parts=1)
    assert sorted(p.name for p in paths[0].parent.iterdir()) == ["orders_part_0.csv"]


def _status(r):
    return r["order_status"].strip().upper()


def _ts(value):
    return value.replace("/", "-")


def test_cancellations_occur_in_batch_and_in_late_updates(batch):
    _, rows = batch
    today = f"ORD-{BATCH_DATE:%Y%m%d}-"
    cancelled_today = {r["order_id"] for r in rows if _status(r) == "CANCELLED" and r["order_id"].startswith(today)}
    cancelled_late = {r["order_id"] for r in rows if _status(r) == "CANCELLED" and r["order_id"].startswith("ORD-20261013-")}
    assert len(cancelled_today) == pytest.approx(ROWS * 0.05 / 2 / 3, rel=0.3)  # a third of the in-batch updates
    assert len(cancelled_late) == pytest.approx(ROWS * 0.01 / 2, rel=0.4)  # half of the late updates


def test_cancelled_is_the_latest_version_of_an_order(batch):
    _, rows = batch
    by_id = {}
    for r in rows:
        by_id.setdefault(r["order_id"], []).append(r)
    for versions in by_id.values():
        if any(_status(v) == "CANCELLED" for v in versions):
            latest = max(versions, key=lambda v: _ts(v["updated_at"]))
            assert _status(latest) == "CANCELLED"


def test_order_timestamp_is_stable_across_versions(batch):
    _, rows = batch
    stamps = {}
    for r in rows:
        stamps.setdefault(r["order_id"], set()).add(_ts(r["order_timestamp"]))
    assert all(len(s) == 1 for s in stamps.values())


def test_late_updates_match_the_previous_days_orders():
    _, day1 = generate_rows(date(2026, 10, 14), 2000, seed=5)
    _, day2 = generate_rows(date(2026, 10, 15), 2000, seed=6)
    originals = {r["order_id"]: r for r in day1}
    late = [r for r in day2 if r["order_id"].startswith("ORD-20261014-") and r["order_id"] in originals]
    assert late
    for r in late:
        o = originals[r["order_id"]]
        assert _ts(r["order_timestamp"]) == _ts(o["order_timestamp"])
        assert r["product_sku"].strip().upper() == o["product_sku"].strip().upper()
        assert r["unit_price"].lstrip("-") == o["unit_price"].lstrip("-")
        assert _ts(r["updated_at"]) > _ts(o["updated_at"])
