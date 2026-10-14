"""Daily legacy HDFS -> GCS -> Databricks medallion pipeline.

verify_hdfs_partition >> distcp_hdfs_to_gcs >> run_databricks_bronze
    >> run_databricks_silver >> run_databricks_gold >> audit_row_counts

Every task is safe to re-run for the same ds: GCS objects are overwritten in
place, Auto Loader skips files it has already ingested, Silver MERGEs by
updated_at and replaces that day's quarantine rows, and Gold is rebuilt in full.

The three Databricks steps trigger the deployed Asset Bundle job
(databricks/databricks.yml, id in DATABRICKS_JOB_ID), one task at a time via
run-now's `only`. The job owns the compute (serverless, STANDARD performance
target), the script paths and bronze_ingest's retries, so the DAG and
`databricks bundle run` share one definition. The audit queries the SQL
Warehouse named by DATABRICKS_WAREHOUSE_ID.

Connections come from AIRFLOW_CONN_* environment variables (docker-compose.yml):
  http_hdfs           WebHDFS on the NameNode
  databricks_default  workspace host + PAT
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.http.sensors.http import HttpSensor

HDFS_SOURCE_PATH = os.environ.get("HDFS_SOURCE_PATH", "/legacy/erp/orders")
NAMENODE_URL = f"http://{os.environ.get('HDFS_NAMENODE_HOST', 'namenode')}:{os.environ.get('HDFS_WEBHDFS_PORT', '9870')}"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
LANDING_BUCKET = os.environ.get("GCS_LANDING_BUCKET", "")
CATALOG = os.environ.get("DATABRICKS_CATALOG", "legacy_migration_cat")
JOB_ID = os.environ.get("DATABRICKS_JOB_ID", "")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

PARTITION = "ingest_date={{ ds }}"


def transfer_partition(ds: str, **_) -> dict:
    from hdfs_to_gcs_transfer import HdfsToGcsBridge

    bridge = HdfsToGcsBridge(NAMENODE_URL, LANDING_BUCKET, gcp_project=GCP_PROJECT_ID)
    manifest = bridge.transfer_partition(f"{HDFS_SOURCE_PATH}/ingest_date={ds}", f"erp/orders/ingest_date={ds}")
    print(f"Transferred {manifest['files_transferred']} file(s), {manifest['total_bytes']} bytes")
    for uri, md5 in manifest["md5_hashes"].items():
        print(f"  {uri}  md5={md5}")
    return manifest  # pushed to XCom


def audit_row_counts(ds: str, **_) -> dict:
    from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

    if not WAREHOUSE_ID:
        raise AirflowFailException("DATABRICKS_WAREHOUSE_ID is not set; the audit needs a SQL Warehouse.")
    sql = f"""
        SELECT
          (SELECT count(*) FROM {CATALOG}.bronze.orders_raw         WHERE batch_date  = DATE'{ds}') AS bronze,
          (SELECT count(*) FROM {CATALOG}.silver.orders_cleansed    WHERE _batch_date = DATE'{ds}') AS silver,
          (SELECT count(*) FROM {CATALOG}.silver.orders_quarantine  WHERE batch_date  = DATE'{ds}') AS quarantine,
          (SELECT count(*) - count(DISTINCT order_id) FROM {CATALOG}.silver.orders_cleansed)        AS silver_dup_keys,
          (SELECT count(*) FROM {CATALOG}.gold.daily_sales_summary)                                  AS gold_daily_rows
    """
    bronze, silver, quarantine, dup_keys, gold_rows = DatabricksSqlHook(
        databricks_conn_id="databricks_default", http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}"
    ).get_first(sql)
    counts = {"bronze": bronze, "silver": silver, "quarantine": quarantine, "silver_dup_keys": dup_keys, "gold_daily_rows": gold_rows}
    print(f"Audit for {ds}: {counts}")

    failures = []
    if bronze == 0:
        failures.append("no Bronze rows for this batch")
    if bronze < silver + quarantine:
        failures.append(f"Bronze ({bronze}) < Silver ({silver}) + Quarantine ({quarantine})")
    if dup_keys:
        failures.append(f"{dup_keys} duplicate order_id(s) in Silver")
    if gold_rows == 0:
        failures.append("Gold daily_sales_summary is empty")
    if failures:
        raise AirflowFailException("Row-count audit failed: " + "; ".join(failures))
    return counts


def databricks_task(task_id: str, job_task_key: str) -> DatabricksRunNowOperator:
    """Run one task of the bundle job for {{ ds }}.

    Provider 6.9.0 has no arguments for job parameters or `only`, so both go in
    through `json`, which the operator merges with job_id.
    """
    return DatabricksRunNowOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        job_id=JOB_ID,
        json={"job_parameters": {"batch_date": "{{ ds }}"}, "only": [job_task_key]},
        polling_period_seconds=30,
    )


with DAG(
    dag_id="legacy_hadoop_to_databricks_lakehouse",
    description="Legacy HDFS ERP orders -> GCS -> Databricks Bronze/Silver/Gold -> BigQuery",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["hadoop", "databricks", "gcp", "migration"],
) as dag:
    verify_hdfs_partition = HttpSensor(
        task_id="verify_hdfs_partition",
        http_conn_id="http_hdfs",
        endpoint=f"webhdfs/v1{HDFS_SOURCE_PATH}/{PARTITION}",
        request_params={"op": "LISTSTATUS"},
        response_check=lambda response: len(response.json()["FileStatuses"]["FileStatus"]) > 0,
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )

    distcp_hdfs_to_gcs = PythonOperator(task_id="distcp_hdfs_to_gcs", python_callable=transfer_partition)

    run_databricks_bronze = databricks_task("run_databricks_bronze", "bronze_ingest")
    run_databricks_silver = databricks_task("run_databricks_silver", "silver_upsert")
    run_databricks_gold = databricks_task("run_databricks_gold", "gold_aggregate")

    audit = PythonOperator(task_id="audit_row_counts", python_callable=audit_row_counts)

    verify_hdfs_partition >> distcp_hdfs_to_gcs >> run_databricks_bronze >> run_databricks_silver >> run_databricks_gold >> audit
