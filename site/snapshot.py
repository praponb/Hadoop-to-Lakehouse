"""Snapshot the migration's numbers into site/public/data.json for the public dashboard page.

Reads three places, so run it after the DAG runs, with the stack up and the CLIs signed in:
  * the `airflow dags test` logs (one file per batch day, named run_YYYY-MM-DD.log) for task
    timings and the audit task's row counts
  * HDFS (WebHDFS on localhost:9870), the GCS landing bucket and seed_data/ to compare every file
  * the SQL warehouse, through the databricksgcp profile, for the layer totals and Gold numbers

Usage: python3 site/snapshot.py LOG_DIR
"""
from __future__ import annotations

import ast
import csv
import glob
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG = "legacy_migration_cat"
WAREHOUSE_ID = "1525087559b13da2"
LANDING = "gs://moukda-lakehouse-landing"
WEBHDFS = "http://localhost:9870/webhdfs/v1/legacy/erp/orders"
BKK = timezone(timedelta(hours=7))

QUERIES = {
    "layer_totals": f"""SELECT
  (SELECT COUNT(*) FROM {CATALOG}.bronze.orders_raw) AS bronze_rows,
  (SELECT COUNT(*) FROM {CATALOG}.silver.orders_cleansed) AS silver_orders,
  (SELECT COUNT(*) FROM {CATALOG}.silver.orders_quarantine) AS quarantine_rows,
  (SELECT COUNT(*) FROM {CATALOG}.gold.daily_sales_summary) AS gold_daily_rows,
  (SELECT COUNT(*) FROM {CATALOG}.gold.customer_ltv) AS gold_customers""",
    "daily_sales": f"""SELECT order_date, SUM(gross_revenue) AS gross_revenue, SUM(net_revenue) AS net_revenue,
  SUM(total_orders) AS total_orders, CAST(ROUND(SUM(total_orders * cancellation_rate)) AS BIGINT) AS cancelled_orders
FROM {CATALOG}.gold.daily_sales_summary GROUP BY order_date ORDER BY order_date""",
    "top_skus": f"""SELECT product_sku, SUM(net_revenue) AS net_revenue, SUM(total_orders) AS total_orders
FROM {CATALOG}.gold.daily_sales_summary GROUP BY product_sku ORDER BY net_revenue DESC LIMIT 10""",
    "ltv_bands": f"""SELECT band, sort_key, COUNT(*) AS customers FROM (
  SELECT CASE WHEN lifetime_spend < 2500 THEN 'Under 2.5k' WHEN lifetime_spend < 5000 THEN '2.5k-5k'
              WHEN lifetime_spend < 10000 THEN '5k-10k' WHEN lifetime_spend < 15000 THEN '10k-15k' ELSE '15k and over' END AS band,
         CASE WHEN lifetime_spend < 2500 THEN 1 WHEN lifetime_spend < 5000 THEN 2
              WHEN lifetime_spend < 10000 THEN 3 WHEN lifetime_spend < 15000 THEN 4 ELSE 5 END AS sort_key
  FROM {CATALOG}.gold.customer_ltv) GROUP BY band, sort_key ORDER BY sort_key""",
    "quarantine_reasons": f"""SELECT reason, COUNT(*) AS rows FROM {CATALOG}.silver.orders_quarantine
LATERAL VIEW explode(split(error_reason, '; ')) t AS reason GROUP BY reason ORDER BY rows DESC""",
}
NUMERIC = re.compile(r"^-?\d+(\.\d+)?$")


def run_logs(log_dir: str) -> tuple[list[dict], list[dict]]:
    """Task minutes and audit counts from each batch day's `airflow dags test` log."""
    parse = lambda s: datetime.fromisoformat(s.replace("+0000", "+00:00"))
    tasks, audits = [], []
    for path in sorted(glob.glob(os.path.join(log_dir, "run_*.log"))):
        day = re.search(r"run_(\d{4}-\d{2}-\d{2})", path).group(1)
        started = {}
        for line in open(path):
            m = re.match(r"\[(\S+)\] \{dag\.py:\d+\} INFO - \[DAG TEST\] (starting|end task) task_id=(\w+)", line)
            if m:
                at, kind, task = parse(m.group(1)), m.group(2), m.group(3)
                if kind == "starting":
                    started[task] = at
                else:
                    tasks.append({"batch_date": day, "task": task,
                                  "started": started[task].astimezone(BKK).isoformat(timespec="seconds"),
                                  "minutes": round((at - started[task]).total_seconds() / 60, 2)})
            m = re.match(r"Audit for (\S+): (\{.*\})", line)
            if m:
                audits.append({"batch_date": m.group(1), **ast.literal_eval(m.group(2))})
    return tasks, audits


def file_copies() -> list[dict]:
    """Every HDFS file beside its copy in the landing bucket; rows come from the identical local file."""
    listing = lambda p: json.load(urllib.request.urlopen(f"{WEBHDFS}{p}?op=LISTSTATUS"))["FileStatuses"]["FileStatus"]
    out = subprocess.run(["gcloud", "storage", "ls", "-l", f"{LANDING}/erp/orders/**"],
                         capture_output=True, text=True, check=True).stdout
    gcs = {p[2].split("/erp/orders/")[1]: int(p[0]) for p in map(str.split, out.splitlines())
           if len(p) == 3 and p[2].startswith("gs://")}
    rows = []
    for part in sorted(d["pathSuffix"] for d in listing("") if d["type"] == "DIRECTORY"):
        for f in sorted(listing(f"/{part}"), key=lambda d: d["pathSuffix"]):
            key = f"{part}/{f['pathSuffix']}"
            local = os.path.join(ROOT, "seed_data", key)
            if os.path.getsize(local) != f["length"]:
                sys.exit(f"{local} differs from HDFS; regenerate seed_data for that day first")
            with open(local, newline="") as fh:
                n = sum(1 for _ in csv.reader(fh)) - 1
            rows.append({"file": key, "batch_date": part.split("=")[1], "rows": n, "hdfs_bytes": f["length"],
                         "gcs_bytes": gcs.get(key), "match": gcs.get(key) == f["length"]})
    return rows


def query(sql: str) -> list[dict]:
    def api(method, path, body=None):
        cmd = ["databricks", "api", method, path, "-p", "databricksgcp"] + (["--json", json.dumps(body)] if body else [])
        env = {k: v for k, v in os.environ.items() if k not in ("DATABRICKS_HOST", "DATABRICKS_TOKEN")}
        return json.loads(subprocess.run(cmd, capture_output=True, text=True, check=True, env=env).stdout)

    r = api("post", "/api/2.0/sql/statements", {"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "50s"})
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = api("get", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r["status"]["state"] != "SUCCEEDED":
        sys.exit(f"Query failed: {r['status']}")
    cols = [c["name"] for c in r["manifest"]["schema"]["columns"]]
    cell = lambda v: float(v) if v is not None and NUMERIC.match(v) else v
    return [dict(zip(cols, map(cell, row))) for row in r.get("result", {}).get("data_array", [])]


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    tasks, audits = run_logs(sys.argv[1])
    data = {"snapshot_at": datetime.now(BKK).isoformat(timespec="minutes"),
            "pipeline_tasks": tasks, "batch_audit": audits, "file_copies": file_copies()}
    for name, sql in QUERIES.items():
        data[name] = query(sql)
    data["layer_totals"] = data["layer_totals"][0]
    with open(os.path.join(ROOT, "site", "public", "data.json"), "w") as f:
        json.dump(data, f, indent=1)
    print(f"site/public/data.json: {len(audits)} batch days, {len(data['file_copies'])} files, "
          f"{len(data['daily_sales'])} order dates")


if __name__ == "__main__":
    main()
