#!/usr/bin/env python3
"""Generate a day of dirty, partitioned legacy ERP order CSVs.

Output layout mirrors the HDFS source:
    <output-dir>/ingest_date=YYYY-MM-DD/orders_part_<n>.csv

Injected legacy anomalies (spec 4.1):
  * ~5% duplicate order_id rows: half exact re-dumps, half status updates with a
    later updated_at. An update moves PENDING/PAID -> SHIPPED, or a third of the
    time two steps, PENDING/PAID -> SHIPPED -> CANCELLED.
  * ~2% null or malformed customer_id.
  * Mixed timestamp formats: 'YYYY-MM-DD HH:MM:SS' and 'YYYY/MM/DD HH:MM:SS'.
  * Late-arriving updates for orders from the previous day, half of which cancel
    the order, so a row that's already merged gets updated again.
  * A few non-positive quantities, negative prices and messy SKU/status casing.
  * --evolve-schema adds a trailing discount_code column (day-2 schema evolution).

Order attributes are derived from the order_id, so an update row for an order
carries the same customer, SKU, quantity, price and order_timestamp as the original.
Standard library only, so it runs with any python3.
"""
from __future__ import annotations

import argparse
import csv
import math
import random
from datetime import date, datetime, timedelta
from pathlib import Path

BASE_COLUMNS = [
    "order_id",
    "customer_id",
    "product_sku",
    "quantity",
    "unit_price",
    "order_status",
    "order_timestamp",
    "updated_at",
]
EVOLVED_COLUMNS = BASE_COLUMNS + ["discount_code"]

TIMESTAMP_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S")
DUPLICATE_RATE = 0.05
BAD_CUSTOMER_RATE = 0.02
LATE_UPDATE_RATE = 0.01
BAD_QUANTITY_RATE = 0.01
NEGATIVE_PRICE_RATE = 0.005
CANCEL_SHARE_IN_BATCH = 1 / 3  # in-batch updates that go all the way to CANCELLED
CANCEL_SHARE_LATE = 0.5  # previous-day updates that cancel the order
ROWS_PER_PART = 5000

SKUS = [f"SKU-{n}" for n in range(1001, 1041)]
MALFORMED_CUSTOMER_IDS = ["N/A", "cust_??", "C-12", "UNKNOWN"]
DISCOUNT_CODES = ["AUTUMN10", "VIP15", "FREESHIP", "WELCOME5"]
# One step of the status lifecycle; an update applies one or two steps.
NEXT_STATUS = {"PENDING": "SHIPPED", "PAID": "SHIPPED", "SHIPPED": "CANCELLED", "CANCELLED": "CANCELLED"}


def order_id_for(batch_date: date, n: int) -> str:
    return f"ORD-{batch_date:%Y%m%d}-{n:06d}"


def _order_attrs(order_id: str) -> dict:
    """Stable per-order attributes, so every version of an order agrees."""
    rng = random.Random(order_id)
    return {
        "customer_id": f"C{rng.randint(1, 5000):06d}",
        "product_sku": rng.choice(SKUS),
        "quantity": rng.randint(1, 10),
        "unit_price": round(rng.uniform(2.0, 500.0), 2),
        "seconds_into_day": rng.randint(0, 86_399 - 3_600),
    }


def _fmt_ts(rng: random.Random, ts: datetime) -> str:
    return ts.strftime(rng.choice(TIMESTAMP_FORMATS))


def _messy(rng: random.Random, value: str) -> str:
    """Legacy systems rarely agree on casing or whitespace."""
    roll = rng.random()
    if roll < 0.05:
        return f"  {value.lower()} "
    if roll < 0.10:
        return value.capitalize()
    return value


def _base_row(rng: random.Random, batch_date: date, order_id: str) -> dict:
    attrs = _order_attrs(order_id)
    order_ts = datetime.combine(batch_date, datetime.min.time()) + timedelta(seconds=attrs["seconds_into_day"])
    updated_ts = order_ts + timedelta(minutes=rng.randint(0, 30))
    return {
        "order_id": order_id,
        "customer_id": attrs["customer_id"],
        "product_sku": _messy(rng, attrs["product_sku"]),
        "quantity": str(attrs["quantity"]),
        "unit_price": f"{attrs['unit_price']:.2f}",
        "order_status": _messy(rng, rng.choice(["PENDING", "PENDING", "PAID"])),
        "order_timestamp": order_ts,
        "updated_at": updated_ts,
    }


def _status_update(rng: random.Random, row: dict, not_before: datetime | None = None, steps: int = 1) -> dict:
    """A later version of `row`, `steps` statuses further on, with a later updated_at."""
    status = row["order_status"].strip().upper()
    for _ in range(steps):
        status = NEXT_STATUS.get(status, "SHIPPED")
    update = dict(row)
    update["order_status"] = status
    start = max(row["updated_at"], not_before) if not_before else row["updated_at"]
    update["updated_at"] = start + timedelta(minutes=rng.randint(1, 600))
    return update


def generate_rows(batch_date: date, rows: int, evolve_schema: bool = False, seed: int | None = None) -> tuple[list[str], list[dict]]:
    """Return (columns, rows) for one batch day. Pure: no filesystem access."""
    if rows < 1:
        raise ValueError("rows must be >= 1")
    rng = random.Random(seed if seed is not None else batch_date.toordinal())

    n_dupes = round(rows * DUPLICATE_RATE)
    n_late = round(rows * LATE_UPDATE_RATE)
    n_base = max(rows - n_dupes - n_late, 1)
    n_dupes = min(n_dupes, rows - n_base)
    n_late = rows - n_base - n_dupes

    out = [_base_row(rng, batch_date, order_id_for(batch_date, i)) for i in range(n_base)]

    # Duplicates of this batch's orders: exact re-dumps and in-batch status updates.
    # Updates build on the order's latest version, so status never moves backwards.
    latest: dict[str, dict] = {}
    for k, source in enumerate(rng.choices(out[:n_base], k=n_dupes)):
        if k % 2 == 0:
            out.append(dict(source))
        else:
            prior = latest.get(source["order_id"], source)
            latest[source["order_id"]] = _status_update(rng, prior, steps=2 if rng.random() < CANCEL_SHARE_IN_BATCH else 1)
            out.append(latest[source["order_id"]])

    # Late-arriving updates for the previous day's orders.
    prev_day = batch_date - timedelta(days=1)
    day_start = datetime.combine(batch_date, datetime.min.time())
    for _ in range(n_late):
        prev = _base_row(rng, prev_day, order_id_for(prev_day, rng.randrange(n_base)))
        prior = latest.get(prev["order_id"], prev)
        latest[prev["order_id"]] = _status_update(rng, prior, not_before=day_start, steps=2 if rng.random() < CANCEL_SHARE_LATE else 1)
        out.append(latest[prev["order_id"]])

    rng.shuffle(out)

    # Dirty customer ids: mostly blank (NULL in CSV), some malformed.
    for i, idx in enumerate(rng.sample(range(len(out)), round(len(out) * BAD_CUSTOMER_RATE))):
        out[idx]["customer_id"] = "" if i % 4 else rng.choice(MALFORMED_CUSTOMER_IDS)
    for idx in rng.sample(range(len(out)), round(len(out) * BAD_QUANTITY_RATE)):
        out[idx]["quantity"] = rng.choice(["0", "-1", "-3"])
    for idx in rng.sample(range(len(out)), round(len(out) * NEGATIVE_PRICE_RATE)):
        out[idx]["unit_price"] = f"-{out[idx]['unit_price'].lstrip('-')}"

    for row in out:
        row["order_timestamp"] = _fmt_ts(rng, row["order_timestamp"])
        row["updated_at"] = _fmt_ts(rng, row["updated_at"])
        if evolve_schema:
            row["discount_code"] = rng.choice(DISCOUNT_CODES) if rng.random() < 0.3 else ""

    return (EVOLVED_COLUMNS if evolve_schema else BASE_COLUMNS), out


def write_partition(output_dir: Path, batch_date: date, columns: list[str], rows: list[dict], parts: int | None = None) -> list[Path]:
    """Write rows as orders_part_<n>.csv under ingest_date=<batch_date>/, replacing old parts."""
    parts = parts or max(1, math.ceil(len(rows) / ROWS_PER_PART))
    partition_dir = output_dir / f"ingest_date={batch_date.isoformat()}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    for stale in partition_dir.glob("orders_part_*.csv"):
        stale.unlink()

    size = math.ceil(len(rows) / parts)
    paths = []
    for n in range(parts):
        path = partition_dir / f"orders_part_{n}.csv"
        with path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows[n * size:(n + 1) * size])
        paths.append(path)
    return paths


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="Batch date, YYYY-MM-DD")
    parser.add_argument("--rows", type=int, default=10000, help="Total rows to emit, anomalies included")
    parser.add_argument("--evolve-schema", action="store_true", help="Add the discount_code column")
    parser.add_argument("--output-dir", type=Path, default=Path("seed_data"))
    parser.add_argument("--parts", type=int, default=None, help=f"CSV part files (default: one per {ROWS_PER_PART} rows)")
    parser.add_argument("--seed", type=int, default=None, help="RNG seed (default: derived from --date)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    columns, rows = generate_rows(args.date, args.rows, args.evolve_schema, args.seed)
    paths = write_partition(args.output_dir, args.date, columns, rows, args.parts)
    print(f"Wrote {len(rows)} rows to {len(paths)} file(s) under {paths[0].parent}")


if __name__ == "__main__":
    main()
