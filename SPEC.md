# Project Specification: Legacy Hadoop-to-Databricks Lakehouse Migration on GCP

> Transcribed from `spec.docx`. Where the implementation deliberately deviates
> from this spec, see [Implementation notes](#implementation-notes) at the end.

## 1. Project Constitution & Agent Guardrails

- **Objective:** Build an end-to-end enterprise migration pipeline that extracts partitioned legacy data from **Hadoop HDFS**, stages it into **Google Cloud Storage (GCS)**, processes it through a **Databricks Medallion Architecture (Bronze -> Silver -> Gold)** using **Apache Spark (PySpark)** and **Delta Lake**, exposes Gold tables to **GCP BigQuery**, and orchestrates the entire lifecycle via **Apache Airflow**.
- **7-Day Trial FinOps Strategy (Hybrid Execution):**
  - **Local Docker Compose:** Run a lightweight single-node **Hadoop HDFS** cluster and **Apache Airflow** locally via Docker to preserve GCP and Databricks trial credits.
  - **Cloud Native (GCP + Databricks):** Use live **Google Cloud Storage (GCS)** buckets, **Databricks on GCP** (Standard/Premium Trial Workspace with Unity Catalog), and **GCP BigQuery**.
- **Coding Standards:**
  - Python 3.11+, PySpark 3.5+ (Databricks Runtime 15.4 LTS or 16.x).
  - Use **Databricks Asset Bundles (`databricks.yml`)** or modular `.py` wheel/spark-submit tasks rather than hardcoded manual UI notebooks.
  - Idempotency required across all stages: re-running an Airflow DAG for the same `logical_date` (`ds`) must not duplicate records in Silver or Gold tables.
  - Never hardcode credentials; load all secrets via `.env` and GCP Service Account JSON keys.

## 2. Target Repository Structure

```text
hadoop-to-databricks-gcp/
├── SPEC.md
├── .env.example
├── .gitignore
├── docker-compose.yml               # Local Hadoop HDFS (NameNode/DataNode) + Airflow
├── Dockerfile.airflow               # Custom Airflow image with GCP & Databricks providers
├── Makefile                         # Shortcut commands for agent verification
├── config/
│   └── gcp-service-account.json     # (Git-ignored) GCP IAM key
├── hadoop_seed/
│   ├── generate_legacy_data.py      # Generates dirty partitioned CSV/JSON data
│   └── load_to_hdfs.sh              # Pushes generated partitions into HDFS /legacy/erp/
├── airflow/
│   ├── dags/
│   │   └── hadoop_to_lakehouse_dag.py
│   └── plugins/
│       └── hdfs_to_gcs_transfer.py  # Streams HDFS partitions to GCS landing zone
├── databricks/
│   ├── databricks.yml               # Databricks Asset Bundle configuration
│   ├── setup_uc.sql                 # Unity Catalog, External Location, and Schema DDL
│   └── src/
│       ├── common/
│       │   └── spark_session.py     # Spark config & widget parameter parser
│       ├── bronze_autoloader.py     # Auto Loader (cloudFiles) GCS -> Bronze Delta
│       ├── silver_transform.py      # Deduplication, type enforcement, SCD/MERGE -> Silver
│       └── gold_aggregations.py     # Star schema & Liquid Clustering -> Gold
├── bigquery/
│   └── create_biglake_tables.sql    # BigQuery External/BigLake table definitions
└── tests/
    ├── test_data_generator.py
    └── test_silver_transformations.py
```

## 3. Environment & Credentials Contract (`.env.example`)

```bash
# Google Cloud Platform
GCP_PROJECT_ID=your-gcp-project-id
GCP_REGION=asia-southeast1
GCS_LANDING_BUCKET=gs://your-lakehouse-landing-bucket
GCS_LAKEHOUSE_BUCKET=gs://your-lakehouse-uc-bucket
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/gcp-service-account.json

# Hadoop HDFS (Local Docker Container)
HDFS_NAMENODE_HOST=namenode
HDFS_WEBHDFS_PORT=9870
HDFS_SOURCE_PATH=/legacy/erp/orders

# Databricks on GCP
DATABRICKS_HOST=https://<your-workspace-id>.gcp.databricks.com
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
DATABRICKS_CLUSTER_ID=XXXX-XXXXXX-XXXXXXXX
DATABRICKS_CATALOG=legacy_migration_cat
```

## 4. Data Contracts & Medallion Transformation Rules

### 4.1 Source Dataset (Hadoop HDFS Legacy ERP Orders)

- **HDFS Path Pattern:** `/legacy/erp/orders/ingest_date=YYYY-MM-DD/orders_part_*.csv`
- **Intentional Legacy Anomalies to Inject in `generate_legacy_data.py`:**
  - Duplicate `order_id` rows (simulating at-least-once legacy batch dumps).
  - Null or malformed `customer_id` (~2% of records).
  - Inconsistent timestamp formats (`YYYY-MM-DD HH:MM:SS` mixed with `YYYY/MM/DD HH:MM:SS`).
  - Late-arriving status updates (`PENDING -> SHIPPED -> CANCELLED`).
  - Schema Evolution trigger: On batch day 2, inject an extra column `discount_code STRING` to test Auto Loader schema evolution.

| Column Name | Raw Type (CSV) | Target Silver Type | Validation / Transformation Rule |
|---|---|---|---|
| `order_id` | STRING | STRING (PK) | Must not be null; deduplicate keeping latest `updated_at` |
| `customer_id` | STRING | STRING | Quarantine to `silver_orders_quarantine` if NULL |
| `product_sku` | STRING | STRING | Uppercase and trim whitespace |
| `quantity` | STRING | INT | Cast to INT; reject/quarantine if less than or equal to 0 |
| `unit_price` | STRING | DECIMAL(10,2) | Cast to DECIMAL(10,2); must be greater than or equal to 0.00 |
| `order_status` | STRING | STRING | Standardize to ('PENDING', 'PAID', 'SHIPPED', 'CANCELLED') |
| `order_timestamp` | STRING | TIMESTAMP | Parse multi-format timestamps into UTC TIMESTAMP |
| `updated_at` | STRING | TIMESTAMP | Used as watermark/ordering column for MERGE INTO |

### 4.2 Medallion Layer Specifications (Databricks + Spark)

**Layer 1: Bronze (`{catalog}.bronze.orders_raw`)**
- Engine: Spark Structured Streaming with Databricks Auto Loader (`format("cloudFiles")`).
- Options:
  - `cloudFiles.format = "csv"`
  - `cloudFiles.inferColumnTypes = "true"`
  - `cloudFiles.schemaEvolutionMode = "addNewColumns"`
  - `cloudFiles.schemaLocation = "{GCS_LAKEHOUSE_BUCKET}/_schemas/bronze_orders"`
- Metadata Columns Added: `_ingest_timestamp` (`current_timestamp()`), `_source_file` (`_metadata.file_path`), `batch_date`.
- Trigger: `trigger(availableNow=True)` for cost-efficient batch-style incremental execution during the 7-day trial.

**Layer 2: Silver (`{catalog}.silver.orders_cleansed` & `{catalog}.silver.orders_quarantine`)**
- Engine: PySpark Batch/Incremental processing reading from Bronze Delta change feed or incremental batch.
- Transformations:
  - Parse timestamps using `coalesce(to_timestamp(col, fmt1), to_timestamp(col, fmt2))`.
  - Split DataFrame using Data Quality rules:
    - Valid rows: `order_id IS NOT NULL AND customer_id IS NOT NULL AND quantity > 0 AND unit_price >= 0`
    - Invalid rows: Append to `{catalog}.silver.orders_quarantine` with `error_reason`.
  - Deduplicate incoming batch over `Window.partitionBy("order_id").orderBy(col("updated_at").desc())`.
  - Upsert into `{catalog}.silver.orders_cleansed` using `DeltaTable.forName(spark, ...).merge()` on `target.order_id = source.order_id` (`WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE SET *`, `WHEN NOT MATCHED THEN INSERT *`).

**Layer 3: Gold (`{catalog}.gold.daily_sales_summary` & `{catalog}.gold.customer_ltv`)**
- Engine: PySpark SQL / DataFrame aggregations.
- Storage Optimization: Enable Delta Liquid Clustering (`CLUSTER BY (order_date, product_sku)`).
- Metrics Calculated:
  - `daily_sales_summary`: `order_date`, `product_sku`, `gross_revenue`, `net_revenue` (excluding CANCELLED), `total_orders`, `cancellation_rate`.
  - `customer_ltv`: `customer_id`, `first_order_date`, `last_order_date`, `lifetime_orders`, `lifetime_spend`.

## 5. Apache Airflow DAG Specification (`hadoop_to_lakehouse_dag.py`)

- **DAG ID:** `legacy_hadoop_to_databricks_lakehouse`
- **Schedule:** `@daily` (`catchup=False`)
- **Task Sequence:**
  1. `verify_hdfs_partition` (HttpSensor or PythonOperator via `hdfs.InsecureClient`): Checks WebHDFS (`http://namenode:9870/webhdfs/v1/legacy/erp/orders/ingest_date={{ ds }}?op=LISTSTATUS`) to verify the daily legacy dump exists.
  2. `distcp_hdfs_to_gcs` (PythonOperator): Streams raw files from HDFS `/legacy/erp/orders/ingest_date={{ ds }}/` directly to `gs://<landing-bucket>/erp/orders/ingest_date={{ ds }}/` using `google-cloud-storage` and `hdfs` libraries, logging file count and byte checksums.
  3. `run_databricks_bronze` (DatabricksSubmitRunOperator): Triggers `bronze_autoloader.py` on the existing Databricks cluster.
  4. `run_databricks_silver` (DatabricksSubmitRunOperator): Triggers `silver_transform.py` with parameter `--batch-date {{ ds }}`.
  5. `run_databricks_gold` (DatabricksSubmitRunOperator): Triggers `gold_aggregations.py`.
  6. `audit_row_counts` (PythonOperator): Queries Databricks SQL / Unity Catalog metadata to verify Bronze >= Silver + Quarantine counts.

## 6. Atomic Implementation Tasks

Execute each task sequentially. Verify each step with its terminal verification command before proceeding to the next task.

### Phase 1: Workspace Scaffolding & Local Docker Infrastructure
- [x] **Task 1.1:** Create the complete directory structure, `.gitignore`, `.env.example`, and `Makefile`. — *Verify:* `ls -la && test -f .env.example`
- [x] **Task 1.2:** Author `docker-compose.yml` containing a lightweight Hadoop 3 HDFS setup (`bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` and datanode) alongside Apache Airflow (`apache/airflow:2.10.2-python3.11` with LocalExecutor and Postgres). — *Verify:* `docker compose config -q`
- [x] **Task 1.3:** Author `Dockerfile.airflow` and `requirements.txt` including `apache-airflow-providers-databricks`, `apache-airflow-providers-google`, `hdfs`, `pyspark==3.5.3`, `delta-spark`, and `pytest`. — *Verify:* `test -f Dockerfile.airflow`

### Phase 2: Legacy Hadoop Data Generator & HDFS Loader
- [x] **Task 2.1:** Implement `hadoop_seed/generate_legacy_data.py` with CLI arguments `--date YYYY-MM-DD`, `--rows 10000`, and `--evolve-schema` (bool). Ensure it injects 5% duplicate order_ids, 2% null customer_ids, and mixed timestamp formats. — *Verify:* `python3 hadoop_seed/generate_legacy_data.py --date 2026-10-14 --rows 100 --output-dir /tmp/test_seed && head -n 5 /tmp/test_seed/ingest_date=2026-10-14/orders_part_0.csv`
- [x] **Task 2.2:** Write unit tests in `tests/test_data_generator.py` verifying duplicate injection, null anomaly percentages, and optional `discount_code` column generation. — *Verify:* `pytest tests/test_data_generator.py`
- [x] **Task 2.3:** Create `hadoop_seed/load_to_hdfs.sh` to upload local generated partitions into the running Docker HDFS NameNode under `/legacy/erp/orders/`. — *Verify:* `bash -n hadoop_seed/load_to_hdfs.sh`

### Phase 3: HDFS-to-GCS Extraction Plugin
- [x] **Task 3.1:** Implement `airflow/plugins/hdfs_to_gcs_transfer.py` with a reusable class `HdfsToGcsBridge` that connects to WebHDFS, reads partitioned files in chunks, uploads blobs to `GCS_LANDING_BUCKET`, and returns a manifest dict (`files_transferred`, `total_bytes`, `md5_hashes`). — *Verify:* `python3 -m py_compile airflow/plugins/hdfs_to_gcs_transfer.py`

### Phase 4: Databricks PySpark Medallion Jobs
- [x] **Task 4.1:** Author `databricks/setup_uc.sql` to create catalog `legacy_migration_cat`, schemas `bronze`, `silver`, and `gold`, and tables with Delta Liquid Clustering.
- [x] **Task 4.2:** Implement `databricks/src/bronze_autoloader.py` using `spark.readStream.format("cloudFiles")` and `.trigger(availableNow=True)`. — *Verify:* `python3 -m py_compile databricks/src/bronze_autoloader.py`
- [x] **Task 4.3:** Implement `databricks/src/silver_transform.py` separating modular transformation functions (`cleanse_and_split_df(df)`) from the `DeltaTable.merge()` execution so transformation logic can be tested locally with pytest.
- [x] **Task 4.4:** Write local PySpark unit tests in `tests/test_silver_transformations.py` testing timestamp parsing, deduplication windowing, and quarantine splitting. — *Verify:* `pytest tests/test_silver_transformations.py`
- [x] **Task 4.5:** Implement `databricks/src/gold_aggregations.py` to build `daily_sales_summary` and `customer_ltv` using `CLUSTER BY`. — *Verify:* `python3 -m py_compile databricks/src/gold_aggregations.py`
- [x] **Task 4.6:** Author `databricks/databricks.yml` (Databricks Asset Bundle) defining the 3-task workflow (`bronze_ingest -> silver_upsert -> gold_aggregate`).

### Phase 5: Apache Airflow Orchestration & BigQuery Integration
- [x] **Task 5.1:** Implement `airflow/dags/hadoop_to_lakehouse_dag.py` chaining WebHDFS check -> HDFS-to-GCS transfer -> Databricks job tasks -> Data quality audit. — *Verify:* `python3 -m py_compile airflow/dags/hadoop_to_lakehouse_dag.py`
- [x] **Task 5.2:** Author `bigquery/create_biglake_tables.sql` to expose the Gold Delta tables (`gs://<lakehouse-bucket>/gold/daily_sales_summary`) in GCP BigQuery.

## Cloud prerequisites (before running Phases 4–5 against live endpoints)

- Two GCS buckets in the GCP project (`landing` and `lakehouse`).
- A GCP Service Account with `Storage Object Admin` and `BigQuery Admin` roles, key saved to `config/gcp-service-account.json`.
  - *As implemented:* the service account has only `Storage Object Admin` on the landing bucket, with no BigQuery Admin (see note 12).
- A Databricks Personal Access Token (`Settings -> Developer -> Access tokens`) and the interactive cluster's `cluster_id`.
  - *As implemented:* no cluster is needed. Jobs run on serverless compute; the SQL Warehouse ID goes in `DATABRICKS_WAREHOUSE_ID` (see note 7).

## Implementation notes

Deliberate deviations from the spec, each made to honour the idempotency guardrail or to make the pipeline actually runnable:

1. **Quarantine is idempotent.** `silver.orders_quarantine` is written with `replaceWhere batch_date = '<ds>'` instead of a blind append, so a re-run replaces that day's rejects.
2. **Silver reads Bronze by `batch_date`** (an incremental batch, which the spec allows) rather than the change feed, so a re-run of `ds` reprocesses exactly the same input.
3. **Silver carries `_batch_date`** (the last batch that touched the row) so the audit can compare per-day counts.
4. **`customer_ltv` clusters by `customer_id`** — it has no `order_date`/`product_sku` columns. `daily_sales_summary` uses `CLUSTER BY (order_date, product_sku)` as specified.
5. **Gold is fully recomputed** from Silver with `INSERT OVERWRITE` on every run.
6. **Gold tables are UC external tables** at `{GCS_LAKEHOUSE_BUCKET}/gold/<table>` with deletion vectors disabled and `delta.checkpointPolicy = 'classic'`, so BigQuery BigLake (`format = 'DELTA_LAKE'`) can read them at a stable path. Databricks enables v2 checkpoints by default on liquid-clustered tables, and BigQuery rejects that reader feature.
7. **Serverless compute instead of a cluster.** The workspace uses serverless jobs compute and a SQL Warehouse, so `DATABRICKS_CLUSTER_ID` from §3 is dropped. The bundle job declares a serverless `environment_key` (environment version 2, Python 3.11) instead of `existing_cluster_id` and uses the cost-optimised `performance_target: STANDARD`. The DAG's three Databricks tasks use `DatabricksRunNowOperator` on that deployed job (`DATABRICKS_JOB_ID`), each with run-now's `only` set to its own task, so the DAG and `databricks bundle run` share one definition, including `bronze_ingest`'s retries. `audit_row_counts` queries the SQL Warehouse set in `DATABRICKS_WAREHOUSE_ID`. Silver doesn't call `.cache()`, which serverless doesn't support.
8. **Schema evolution reaches Silver without auto-merge:** `silver.orders_cleansed` is created with a nullable `discount_code`; batches from before day 2 get it as NULL. When Auto Loader reports the new column it stops the stream and Databricks fails the task; a task retry (`max_retries: 2` on `bronze_ingest`, and the Airflow task's retries) picks up the evolved schema.
9. **Extra Silver quality rules:** besides the spec's four, rows are quarantined for malformed `customer_id` (not `C` + 6 digits), unparseable quantity/price/timestamps and unknown statuses. All failed rules are listed in `error_reason`.
10. **Local tooling:** PySpark 3.5 needs Python ≤ 3.12, so `make venv` builds `.venv` with Python 3.11. Airflow's constraints file pins `pyspark==3.5.2`, so that one line is dropped to honour the spec's `pyspark==3.5.3`.
11. **Apple-silicon hosts:** the `bde2020` Hadoop images are amd64-only, so `docker-compose.yml` pins `platform: linux/amd64`.
12. **Least-privilege service account.** `airflow-lakehouse` has only `roles/storage.objectAdmin` on the landing bucket, which is all the HDFS-to-GCS transfer needs. It doesn't get `BigQuery Admin`: BigQuery reads Gold through the `biglake_gcs` connection, whose own service account has `roles/storage.objectViewer` on the lakehouse bucket.
13. **No runtime version pin.** Serverless compute doesn't let you choose a Databricks Runtime, so the §1 "15.4 LTS or 16.x" can't be pinned. `environment_version: "2"` fixes Python 3.11 only; Databricks manages the Spark runtime.
14. **Bad `customer_id` split.** Task 2.1 asks for 2% null customer_ids; the generator follows §4.1 ("null or malformed, ~2%") instead: 1.5% blank (NULL in CSV) plus 0.5% malformed. Both kinds are quarantined in Silver (note 9).
15. **Folder layout.** The repo root is `Hadoop-to-Lakehouse/`, not `hadoop-to-databricks-gcp/`. Files beyond the §2 tree: `hadoop.env`, `requirements.txt` (required by Task 1.3), `tests/conftest.py`, `tests/test_gold_aggregations.py`, `tests/test_hdfs_to_gcs_transfer.py`, `tests/test_dag_integrity.py`, `README.md`, `pending-item.md` and `trial-end-cleanup.md`.
16. **Verification commands and timestamp parsing.** The `pytest …` commands in §6 need the project venv: run `make test` or `.venv/bin/pytest` (note 10). Timestamps are parsed with `try_to_timestamp` instead of `to_timestamp`: the result is the same, but an unparseable value becomes NULL (and is quarantined) instead of failing the job when ANSI mode is on.
