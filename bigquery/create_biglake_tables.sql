-- Expose the Databricks Gold Delta tables to BigQuery as BigLake tables.
-- BigQuery reads the Delta log at query time, so each Gold rebuild shows up
-- without recreating these tables.
--
-- Values match .env.example: project moukda-lakehouse, region asia-southeast1,
-- lakehouse bucket gs://moukda-lakehouse-uc, dataset lakehouse_gold,
-- connection biglake_gcs.
--
-- One-time setup (shell):
--   bq mk --connection --location=asia-southeast1 --project_id=moukda-lakehouse \
--     --connection_type=CLOUD_RESOURCE biglake_gcs
--   bq show --connection moukda-lakehouse.asia-southeast1.biglake_gcs   # prints serviceAccountId
--   gcloud storage buckets add-iam-policy-binding gs://moukda-lakehouse-uc \
--     --member=serviceAccount:<serviceAccountId> --role=roles/storage.objectViewer
--
-- Then: bq query --use_legacy_sql=false < bigquery/create_biglake_tables.sql

CREATE SCHEMA IF NOT EXISTS `moukda-lakehouse.lakehouse_gold`
  OPTIONS (location = 'asia-southeast1', description = 'Gold tables from the Databricks lakehouse (BigLake over Delta)');

CREATE OR REPLACE EXTERNAL TABLE `moukda-lakehouse.lakehouse_gold.daily_sales_summary`
  WITH CONNECTION `moukda-lakehouse.asia-southeast1.biglake_gcs`
  OPTIONS (
    format = 'DELTA_LAKE',
    uris = ['gs://moukda-lakehouse-uc/gold/daily_sales_summary']
  );

CREATE OR REPLACE EXTERNAL TABLE `moukda-lakehouse.lakehouse_gold.customer_ltv`
  WITH CONNECTION `moukda-lakehouse.asia-southeast1.biglake_gcs`
  OPTIONS (
    format = 'DELTA_LAKE',
    uris = ['gs://moukda-lakehouse-uc/gold/customer_ltv']
  );

-- Smoke test:
-- SELECT order_date, SUM(net_revenue) AS net_revenue, SUM(total_orders) AS orders
-- FROM `moukda-lakehouse.lakehouse_gold.daily_sales_summary`
-- GROUP BY order_date ORDER BY order_date;
