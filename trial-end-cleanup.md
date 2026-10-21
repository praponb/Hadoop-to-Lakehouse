# Trial-end cleanup

Teardown of everything the Hadoop-to-Lakehouse project creates.

- **First teardown, 2026-10-20, 07:03–07:06 Bangkok time:** every section was run and its check passed.
- **Rebuilt on 2026-10-21** with mock data for four batch days (2026-10-14 to 2026-10-17), so the dashboards
  have data again. BigQuery (section 1) was not rebuilt. The ids below are the rebuilt ones, and the
  boxes are open again.

The GCP project `moukda-lakehouse` itself is kept (see *Keep*).

Work through the sections in order: each one removes things the next one depends on.

| Where | Resource |
|---|---|
| GCP project | `moukda-lakehouse` (asia-southeast1) |
| Databricks workspace | `https://8259554454724321.1.gcp.databricks.com` (CLI profile `databricksgcp`) |
| SQL warehouse | `1525087559b13da2` (Serverless Starter Warehouse) |
| Cloudflare | Worker `lakehouse-dashboard` on the custom domain `lakehouse.praponb.com` (account of prapon@gmail.com) |

---

## 1. BigQuery

Not rebuilt on 2026-10-21: skip this section unless you have run `bigquery/create_biglake_tables.sql` since.

- [x] Delete the dataset `lakehouse_gold` (BigLake tables `daily_sales_summary` and `customer_ltv`)
- [x] Delete the connection `biglake_gcs` (this also removes its `bqcx-…` service account)

```bash
bq rm -r -f --dataset moukda-lakehouse:lakehouse_gold
bq rm -f --connection moukda-lakehouse.asia-southeast1.biglake_gcs
```

Check: `bq ls --project_id=moukda-lakehouse` lists no datasets.

## 2. Databricks

- [ ] Destroy the bundle: the job `[dev <your-user>] legacy_medallion` (id `573868867052952`), the AI/BI
      dashboard `[dev <your-user>] Legacy lakehouse overview` (id `01f1ccf91dba1a04aadf207a510d5bc1`) and the
      workspace files under `/Workspace/Users/<your-user>/.bundle/hadoop_to_lakehouse/dev`
- [ ] Drop the catalog `legacy_migration_cat` and everything in it (bronze, silver and gold schemas and tables)
- [ ] Drop the external locations `landing_zone` and `lakehouse_zone`
- [ ] Drop the storage credential `lakehouse_gcs_cred` (GCP SA `db-uc-credential-06glole4ek-2o@uc-asias1.iam.gserviceaccount.com`)

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

`bundle destroy` moves the dashboard to the workspace Trash, where its published page still opens (with every
widget failing) until Trash empties it after 30 days. Delete it from Trash to remove it sooner.

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
      its JSON key, id `f30563e8c07ef1bcb05008f9b56d847c0a9db654`.
- [ ] Delete the local key file `config/gcp-service-account.json`

```bash
gcloud iam service-accounts delete airflow-lakehouse@moukda-lakehouse.iam.gserviceaccount.com --project=moukda-lakehouse --quiet
rm config/gcp-service-account.json
```

Check: `gcloud iam service-accounts list --project=moukda-lakehouse` shows only the default compute SA.

## 5. Databricks access token

- [ ] Revoke the PAT that Airflow uses from `.env` (comment `moukda-lakehouse`, created 2026-10-21 09:40
      Bangkok time, expires 2026-11-04 09:40) under Settings → Developer → Access tokens
- [ ] Remove `DATABRICKS_TOKEN` from `.env`, and sign the CLI out with
      `databricks auth logout --profile databricksgcp` (the profile now uses OAuth, not a token)

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

## 7. Cloudflare page

`lakehouse.praponb.com` is a static Worker that serves `site/public/`: the dashboard page and its numbers
snapshot. It doesn't depend on anything above, so it keeps working after sections 1–6. Remove it only when
the public page should go too.

- [ ] Delete the Worker `lakehouse-dashboard`. This also removes its custom domain and the DNS record
      Cloudflare made for it.

```bash
cd site && npx wrangler@4.20.0 delete
```

Check: `dig +short lakehouse.praponb.com` returns nothing.

The first teardown (2026-10-20) instead removed a redirect rule *lakehouse to Databricks dashboard* and a
placeholder record `AAAA lakehouse → 100::`. Neither exists any more.

---

## Keep

- The GitHub repo `praponb/Hadoop-to-Lakehouse` (public; branches `main` and `b_pbv_main`)
- The GCP project `moukda-lakehouse` itself, unless you want to shut it down too
