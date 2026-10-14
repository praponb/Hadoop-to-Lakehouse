# Trial-end cleanup

Pending teardown of everything the Hadoop-to-Lakehouse trial created. Status as of
2026-10-14 15:30 Bangkok time: **nothing below has been done yet.**

Nothing is running or scheduled. The Airflow DAG is paused and the Databricks job
has no schedule, so the only ongoing costs are GCS storage and the idle local
containers.

Work through the sections in order: each one removes things the next one depends on.

| Where | Resource |
|---|---|
| GCP project | `moukda-lakehouse` (asia-southeast1) |
| Databricks workspace | `https://8259554454724321.1.gcp.databricks.com` (CLI profile `databricksgcp`) |
| SQL warehouse | `1525087559b13da2` (Serverless Starter Warehouse) |

---

## 1. BigQuery

- [ ] Delete the dataset `lakehouse_gold` (BigLake tables `daily_sales_summary` and `customer_ltv`)
- [ ] Delete the connection `biglake_gcs` (this also removes its `bqcx-…` service account)

```bash
bq rm -r -f --dataset moukda-lakehouse:lakehouse_gold
bq rm -f --connection moukda-lakehouse.asia-southeast1.biglake_gcs
```

Check: `bq ls --project_id=moukda-lakehouse` lists no datasets.

## 2. Databricks

- [ ] Destroy the bundle: the job `[dev <your-user>] legacy_medallion` (id `470491837537884`) and the
      workspace files under `/Workspace/Users/<your-user>/.bundle/hadoop_to_lakehouse/dev`
- [ ] Drop the catalog `legacy_migration_cat` and everything in it (bronze, silver and gold schemas and tables)
- [ ] Drop the external locations `landing_zone` and `lakehouse_zone`
- [ ] Drop the storage credential `lakehouse_gcs_cred` (GCP SA `db-uc-credential-…@uc-asias1.iam.gserviceaccount.com`)

```bash
cd databricks && env -u DATABRICKS_HOST -u DATABRICKS_TOKEN databricks bundle destroy -t dev --profile databricksgcp
```

Then in the SQL editor on the warehouse, in this order:

```sql
DROP CATALOG IF EXISTS legacy_migration_cat CASCADE;
DROP EXTERNAL LOCATION IF EXISTS landing_zone;
DROP EXTERNAL LOCATION IF EXISTS lakehouse_zone;
DROP STORAGE CREDENTIAL IF EXISTS lakehouse_gcs_cred;
```

Check: `databricks catalogs list --profile databricksgcp` no longer shows `legacy_migration_cat`, and
`databricks storage-credentials list --profile databricksgcp` shows only the Databricks-managed credential.

## 3. Google Cloud Storage

- [ ] Delete `gs://moukda-lakehouse-landing` (landing CSVs)
- [ ] Delete `gs://moukda-lakehouse-uc` (managed tables, Gold Delta files, Auto Loader schemas and checkpoints)

```bash
gcloud storage rm -r gs://moukda-lakehouse-landing gs://moukda-lakehouse-uc
```

Deleting a bucket also removes its IAM bindings (Databricks credential, BigQuery connection, Airflow SA).
Soft-deleted objects stay recoverable, and billed, for the default 7 days.

Check: `gcloud storage buckets list --project=moukda-lakehouse` lists nothing.

## 4. Service account and key

- [ ] Delete the service account `airflow-lakehouse@moukda-lakehouse.iam.gserviceaccount.com`. This also deletes
      its JSON key, id `69733a77480e78da901db5d4decbc31271d76201`.
- [ ] Delete the local key file `config/gcp-service-account.json`

```bash
gcloud iam service-accounts delete airflow-lakehouse@moukda-lakehouse.iam.gserviceaccount.com --project=moukda-lakehouse --quiet
rm config/gcp-service-account.json
```

Check: `gcloud iam service-accounts list --project=moukda-lakehouse` shows only the default compute SA.

## 5. Databricks access token

- [ ] Revoke the PAT used by `.env` and the `databricksgcp` profile (comment `moukda-lakehouse`, created
      2026-10-14 11:57 Bangkok time, expires 2026-10-28) under Settings → Developer → Access tokens
- [ ] Remove `DATABRICKS_TOKEN` from `.env`, and the token from the `databricksgcp` profile in `~/.databrickscfg`

Do this after section 2, because the bundle destroy and the SQL statements need the token.

## 6. Local machine

- [ ] Stop the stack and delete its volumes (HDFS data, Airflow metadata DB, logs)
- [ ] Optionally remove the custom Airflow image, the Python venv and the generated data

```bash
docker compose down -v
docker image rm hadoop-to-lakehouse/airflow:2.10.2
rm -rf .venv seed_data
```

The `bde2020/hadoop-*`, `apache/airflow` and `postgres` base images can be removed with `docker image rm` too,
if nothing else uses them.

---

## Keep

- The GitHub repo `praponb/Hadoop-to-Lakehouse` (public; branches `main` and `b_pbv_main`)
- The GCP project `moukda-lakehouse` itself, unless you want to shut it down too
