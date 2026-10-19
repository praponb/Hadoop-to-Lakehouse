# Pending items

Open work for the Hadoop-to-Lakehouse migration. It comes from comparing `spec.docx`, `SPEC.md` and the repo
on 2026-10-14, about 15:45 Bangkok time.

Every requirement of the spec is implemented and verified against the live services: §1 guardrails, the §4
data contracts and medallion rules, the §5 DAG, and §6 tasks 1.1–5.2. The items below are gaps in the
spec's folder-tree comments, deviations that SPEC.md doesn't document yet, untested paths and cleanup.
None of them blocks the pipeline.

Work on branch `b_pbv_main`. The repo is public, so scan for secrets before pushing.

---

## A. Built smaller than the spec's folder-tree comments suggest

All three are intentionally left out, as recorded in `SPEC.md` implementation notes 18–20.

- [x] **A1. Gold star schema.** The tree comment on `databricks/src/gold_aggregations.py` says
      "Star schema & Liquid Clustering -> Gold". Only the two aggregates that §4.2 defines exist
      (`daily_sales_summary` and `customer_ltv`). There are no dimension or fact tables such as
      `dim_customer`, `dim_product` or `fact_orders`.
- [x] **A2. JSON source data.** The tree comment on `hadoop_seed/generate_legacy_data.py` says
      "Generates dirty partitioned CSV/JSON data". Only CSV is generated, which matches the §4.1 path
      pattern `orders_part_*.csv`.
- [x] **A3. Widget parameter parser.** The tree comment on `databricks/src/common/spark_session.py` says
      "Spark config & widget parameter parser". Parameters are read with argparse only, with no `dbutils.widgets`.
      Widgets apply only to notebooks; the jobs are `spark_python_task` scripts.

## B. Deviations not yet documented in SPEC.md

Added to `SPEC.md` as implementation notes 12–16, plus an *As implemented* line under the prerequisites for B4.

- [x] **B4. No BigQuery Admin.** The spec's prerequisites ask for a service account with `Storage Object Admin`
      and `BigQuery Admin`. Implemented: `airflow-lakehouse` has only `roles/storage.objectAdmin` on the landing
      bucket. BigQuery reads Gold through the `biglake_gcs` connection, whose service account has
      `roles/storage.objectViewer` on the lakehouse bucket. Add an *As implemented* line under the prerequisites.
- [x] **B5. Databricks Runtime 15.4 LTS / 16.x.** Serverless doesn't allow choosing a runtime version.
      `environment_version: "2"` fixes only Python 3.11; Databricks manages the Spark runtime.
- [x] **B6. "2% null customer_ids" (Task 2.1).** The generator follows §4.1 instead ("null or malformed, ~2%"):
      1.5% null plus 0.5% malformed.
- [x] **B7. Folder layout.** The root is `Hadoop-to-Lakehouse/`, not `hadoop-to-databricks-gcp/`. Extra files not in
      the §2 tree: `hadoop.env`, `requirements.txt` (which Task 1.3 requires), `tests/conftest.py`,
      `tests/test_gold_aggregations.py`, `tests/test_hdfs_to_gcs_transfer.py`, `tests/test_dag_integrity.py`,
      `README.md`, `trial-end-cleanup.md` and this file.
- [x] **B8. Spec verification commands and timestamp parsing.** The `pytest …` commands need the venv:
      `make test` or `.venv/bin/pytest`. Timestamps are parsed with `try_to_timestamp`, not `to_timestamp`.
      The result is the same, but it returns NULL instead of failing when ANSI mode is on.

## C. Not yet tested end to end

- [ ] **C1. A scheduled DAG run.** The full DAG was verified with `airflow dags test … 2026-10-14`, with the DAG
      paused. Running it through the scheduler means unpausing, which immediately creates a run for the
      latest interval. That needs an HDFS partition for that date, or the sensor waits an hour and fails.
- [ ] **C2. Bronze schema-change retry through Airflow.** When a new column appears, the bundle job's
      `max_retries` recovered (tested on day 2). The Airflow path relies on the DAG's `retries: 1`, triggering
      `bronze_ingest` again, which hasn't been exercised.

## D. Housekeeping

- [x] **D1. Commit and push** `README.md`, `trial-end-cleanup.md` and `pending-item.md` to `b_pbv_main`, if wanted.
      Done in commit `0a76445`, pushed to `origin/b_pbv_main`.
- [x] **D2. Unused `.env` keys.** No code reads `GCP_REGION`, `BQ_DATASET` or `BQ_CONNECTION`: the region, dataset
      and connection are written out in `bigquery/create_biglake_tables.sql` and `databricks/setup_uc.sql`. Either
      use them, for example a `make bq-setup` target that substitutes them, or drop them from `.env.example`.
      Dropped from `.env.example`, `.env` and the README table; SPEC.md note 17.

## E. Trial-end cleanup

- [ ] **E1. Tear down** the cloud and local resources. The steps, commands and checks are in
      [`trial-end-cleanup.md`](trial-end-cleanup.md):
  1. BigQuery: dataset `lakehouse_gold` and connection `biglake_gcs`
  2. Databricks: bundle destroy, catalog `legacy_migration_cat`, external locations `landing_zone` and
     `lakehouse_zone`, storage credential `lakehouse_gcs_cred`
  3. GCS: buckets `moukda-lakehouse-landing` and `moukda-lakehouse-uc`
  4. Service account `airflow-lakehouse` (and its key `69733a77…`), plus the local key file
  5. The Databricks PAT `moukda-lakehouse` (expires 2026-10-28)
  6. Local: `docker compose down -v`, image, `.venv`, `seed_data/`
