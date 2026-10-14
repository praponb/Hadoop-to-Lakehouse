-- Unity Catalog setup for the Hadoop-to-Lakehouse migration. Run once, in order,
-- in the Databricks SQL editor on your SQL Warehouse.
--
-- Bucket names match .env.example. If yours differ, replace
--   moukda-lakehouse-landing  (GCS_LANDING_BUCKET)
--   moukda-lakehouse-uc       (GCS_LAKEHOUSE_BUCKET)
--
-- Prerequisite (UI/CLI only; SQL can't create GCP storage credentials):
--   1. Create a "GCP Service Account" credential named lakehouse_gcs_cred, in
--      Catalog Explorer > External data > Credentials, or with
--        databricks storage-credentials create --json \
--          '{"name": "lakehouse_gcs_cred", "databricks_gcp_service_account": {}}'
--      Databricks generates a Google service account email for it.
--   2. Grant that email roles/storage.objectAdmin and
--      roles/storage.legacyBucketReader on BOTH buckets, e.g.
--        gcloud storage buckets add-iam-policy-binding gs://moukda-lakehouse-uc \
--          --member=serviceAccount:<generated-email> --role=roles/storage.objectAdmin
--
-- Lakehouse bucket layout:
--   managed/       UC managed storage (bronze, silver)
--   gold/<table>   external Gold tables, read by BigQuery BigLake
--   _schemas/      Auto Loader schema tracking
--   _checkpoints/  streaming checkpoints

-- 1. External locations -----------------------------------------------------------
CREATE EXTERNAL LOCATION IF NOT EXISTS landing_zone
  URL 'gs://moukda-lakehouse-landing/'
  WITH (STORAGE CREDENTIAL lakehouse_gcs_cred)
  COMMENT 'Raw HDFS extracts landed by Airflow';

CREATE EXTERNAL LOCATION IF NOT EXISTS lakehouse_zone
  URL 'gs://moukda-lakehouse-uc/'
  WITH (STORAGE CREDENTIAL lakehouse_gcs_cred)
  COMMENT 'Managed storage, Gold external tables, Auto Loader state';

-- 2. Catalog and schemas ------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS legacy_migration_cat
  MANAGED LOCATION 'gs://moukda-lakehouse-uc/managed'
  COMMENT 'Legacy Hadoop ERP migration (medallion)';

CREATE SCHEMA IF NOT EXISTS legacy_migration_cat.bronze COMMENT 'Raw Auto Loader ingests, schema-on-read';
CREATE SCHEMA IF NOT EXISTS legacy_migration_cat.silver COMMENT 'Cleansed, deduplicated, typed orders';
CREATE SCHEMA IF NOT EXISTS legacy_migration_cat.gold   COMMENT 'Business aggregates exposed to BigQuery';

-- legacy_migration_cat.bronze.orders_raw is created by bronze_autoloader.py from the inferred CSV schema.

-- 3. Silver ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS legacy_migration_cat.silver.orders_cleansed (
  order_id        STRING NOT NULL COMMENT 'Primary key',
  customer_id     STRING NOT NULL,
  product_sku     STRING,
  quantity        INT,
  unit_price      DECIMAL(10,2),
  order_status    STRING COMMENT 'PENDING | PAID | SHIPPED | CANCELLED',
  order_timestamp TIMESTAMP COMMENT 'UTC',
  updated_at      TIMESTAMP COMMENT 'UTC; MERGE ordering column',
  discount_code   STRING COMMENT 'Added by the day-2 legacy schema change',
  _batch_date     DATE COMMENT 'ingest_date of the batch that last wrote this row',
  CONSTRAINT orders_cleansed_pk PRIMARY KEY (order_id) RELY
)
CLUSTER BY (order_id)
COMMENT 'One row per order, latest updated_at wins';

CREATE TABLE IF NOT EXISTS legacy_migration_cat.silver.orders_quarantine (
  order_id        STRING,
  customer_id     STRING,
  product_sku     STRING,
  quantity        STRING,
  unit_price      STRING,
  order_status    STRING,
  order_timestamp STRING,
  updated_at      STRING,
  discount_code   STRING,
  error_reason    STRING COMMENT '; -separated failed rules',
  _source_file    STRING,
  batch_date      DATE
)
CLUSTER BY (batch_date)
COMMENT 'Rows that failed Silver data-quality rules; replaced per batch_date';

-- 4. Gold (external, at stable paths BigQuery can read) --------------------------------------
-- BigQuery's Delta Lake reader supports neither deletion vectors nor v2 checkpoints,
-- both of which Databricks turns on by default for liquid-clustered tables, so both
-- are switched off here. Classic checkpoints keep the read protocol at version 1.
CREATE TABLE IF NOT EXISTS legacy_migration_cat.gold.daily_sales_summary (
  order_date        DATE,
  product_sku       STRING,
  gross_revenue     DECIMAL(18,2),
  net_revenue       DECIMAL(18,2) COMMENT 'Excludes CANCELLED orders',
  total_orders      BIGINT,
  cancellation_rate DECIMAL(5,4)
)
CLUSTER BY (order_date, product_sku)
LOCATION 'gs://moukda-lakehouse-uc/gold/daily_sales_summary'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.checkpointPolicy' = 'classic');

CREATE TABLE IF NOT EXISTS legacy_migration_cat.gold.customer_ltv (
  customer_id      STRING,
  first_order_date DATE,
  last_order_date  DATE,
  lifetime_orders  BIGINT,
  lifetime_spend   DECIMAL(18,2) COMMENT 'Excludes CANCELLED orders'
)
CLUSTER BY (customer_id)
LOCATION 'gs://moukda-lakehouse-uc/gold/customer_ltv'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false', 'delta.checkpointPolicy' = 'classic');
