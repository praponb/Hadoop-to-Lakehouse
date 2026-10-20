# Trial-end cleanup

Teardown of everything the Hadoop-to-Lakehouse trial created. **Done on 2026-10-20,
07:03–07:06 Bangkok time**: every section below was run and its check passed.

Nothing is left running or billed. The GCP project `moukda-lakehouse` is kept, empty of
pipeline resources (see *Keep*).

Work through the sections in order: each one removes things the next one depends on.

| Where | Resource |
|---|---|
| GCP project | `moukda-lakehouse` (asia-southeast1) |
| Databricks workspace | `https://8259554454724321.1.gcp.databricks.com` (CLI profile `databricksgcp`) |
| SQL warehouse | `1525087559b13da2` (Serverless Starter Warehouse) |
| Cloudflare zone | `praponb.com`, redirect `lakehouse.praponb.com` → dashboard |

---

## 1. BigQuery

- [x] Delete the dataset `lakehouse_gold` (BigLake tables `daily_sales_summary` and `customer_ltv`)
- [x] Delete the connection `biglake_gcs` (this also removes its `bqcx-…` service account)

```bash
bq rm -r -f --dataset moukda-lakehouse:lakehouse_gold
bq rm -f --connection moukda-lakehouse.asia-southeast1.biglake_gcs
```

Check: `bq ls --project_id=moukda-lakehouse` lists no datasets.

## 2. Databricks

- [x] Destroy the bundle: the job `[dev <your-user>] legacy_medallion` (id `470491837537884`), the AI/BI
      dashboard `[dev <your-user>] Legacy lakehouse overview` (id `01f1cba154791147ba8e74d8e7d22b52`) and the
      workspace files under `/Workspace/Users/<your-user>/.bundle/hadoop_to_lakehouse/dev`
- [x] Drop the catalog `legacy_migration_cat` and everything in it (bronze, silver and gold schemas and tables)
- [x] Drop the external locations `landing_zone` and `lakehouse_zone`
- [x] Drop the storage credential `lakehouse_gcs_cred` (GCP SA `db-uc-credential-…@uc-asias1.iam.gserviceaccount.com`)

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

`DROP EXTERNAL LOCATION lakehouse_zone` fails while Unity Catalog still counts the dropped catalog's managed
tables (kept for its undrop window). Use `DROP EXTERNAL LOCATION IF EXISTS lakehouse_zone FORCE`: it leaves those
files in `gs://…-uc`, which section 3 deletes with the bucket.

Check: `databricks catalogs list --profile databricksgcp` no longer shows `legacy_migration_cat`, and
`databricks storage-credentials list --profile databricksgcp` shows only the Databricks-managed credential.

## 3. Google Cloud Storage

- [x] Delete `gs://moukda-lakehouse-landing` (landing CSVs)
- [x] Delete `gs://moukda-lakehouse-uc` (managed tables, Gold Delta files, Auto Loader schemas and checkpoints)

```bash
gcloud storage rm -r gs://moukda-lakehouse-landing gs://moukda-lakehouse-uc
```

Deleting a bucket also removes its IAM bindings (Databricks credential, BigQuery connection, Airflow SA).
Soft-deleted objects stay recoverable, and billed, for the default 7 days.

Check: `gcloud storage buckets list --project=moukda-lakehouse` lists nothing.

## 4. Service account and key

- [x] Delete the service account `airflow-lakehouse@moukda-lakehouse.iam.gserviceaccount.com`. This also deletes
      its JSON key, id `69733a77480e78da901db5d4decbc31271d76201`.
- [x] Delete the local key file `config/gcp-service-account.json`

```bash
gcloud iam service-accounts delete airflow-lakehouse@moukda-lakehouse.iam.gserviceaccount.com --project=moukda-lakehouse --quiet
rm config/gcp-service-account.json
```

Check: `gcloud iam service-accounts list --project=moukda-lakehouse` shows only the default compute SA.

## 5. Databricks access token

- [x] Revoke the PAT used by `.env` and the `databricksgcp` profile (comment `moukda-lakehouse`, created
      2026-10-14 11:57 Bangkok time, expires 2026-10-28) under Settings → Developer → Access tokens
- [x] Remove `DATABRICKS_TOKEN` from `.env`, and the token from the `databricksgcp` profile in `~/.databrickscfg`

Do this after section 2, because the bundle destroy and the SQL statements need the token.

## 6. Local machine

- [x] Stop the stack and delete its volumes (HDFS data, Airflow metadata DB, logs)
- [x] Optionally remove the custom Airflow image, the Python venv and the generated data

```bash
docker compose down -v
docker image rm hadoop-to-lakehouse/airflow:2.10.2
rm -rf .venv seed_data
```

The `bde2020/hadoop-*`, `apache/airflow` and `postgres` base images can be removed with `docker image rm` too,
if nothing else uses them.

## 7. Cloudflare redirect

`lakehouse.praponb.com` redirects to the dashboard, which section 2 deletes. Afterwards the link leads to a
Databricks error page, so remove the redirect (or point it somewhere else).

- [x] Delete the redirect rule *lakehouse to Databricks dashboard* (praponb.com → Rules → Redirect Rules).
      It matches `http*://lakehouse.praponb.com/*` and sends a 302 to the dashboard's published URL.
- [x] Delete the DNS record `AAAA lakehouse → 100::` (proxied) under praponb.com → DNS → Records.
      It is a placeholder that exists only for the redirect.

Check: `dig +short lakehouse.praponb.com @nataly.ns.cloudflare.com` returns nothing, and
`curl -sI https://lakehouse.praponb.com` no longer answers with a 302 to Databricks.

To keep the link instead, edit the rule's target URL. A redeployed bundle creates a dashboard with a new id.

---

## Keep

- The GitHub repo `praponb/Hadoop-to-Lakehouse` (public; branches `main` and `b_pbv_main`)
- The GCP project `moukda-lakehouse` itself, unless you want to shut it down too
