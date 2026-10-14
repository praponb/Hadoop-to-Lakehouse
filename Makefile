# Shortcut commands for local verification. Python work runs in a 3.11 venv
# because PySpark 3.5 does not support newer interpreters.

-include .env
export

PYTHON_BIN ?= /opt/homebrew/bin/python3.11
VENV       := .venv
PY         := $(VENV)/bin/python
AIRFLOW_VERSION := 2.10.2
CONSTRAINTS_URL := https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-3.11.txt

DATABRICKS_PROFILE ?= databricksgcp

SEED_DIR ?= seed_data
DATE     ?= 2026-10-14
ROWS     ?= 10000

.PHONY: help venv test compile config up down logs seed load-hdfs hdfs-ls bundle-validate bundle-deploy clean

help:
	@grep -E '^[a-z-]+:' Makefile | cut -d: -f1

venv: $(VENV)/.installed

$(VENV)/.installed: requirements.txt
	uv venv --python $(PYTHON_BIN) $(VENV)
	curl -sSfL $(CONSTRAINTS_URL) | grep -v '^pyspark==' > $(VENV)/constraints.txt
	uv pip install --python $(PY) "apache-airflow==$(AIRFLOW_VERSION)" -r requirements.txt -c $(VENV)/constraints.txt
	touch $@

test: venv
	$(PY) -m pytest tests/ -v

compile:
	python3 -m py_compile hadoop_seed/generate_legacy_data.py airflow/plugins/hdfs_to_gcs_transfer.py \
		airflow/dags/hadoop_to_lakehouse_dag.py databricks/src/common/spark_session.py \
		databricks/src/bronze_autoloader.py databricks/src/silver_transform.py databricks/src/gold_aggregations.py
	bash -n hadoop_seed/load_to_hdfs.sh

config:
	docker compose config -q

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

# Generate a day of dirty legacy data; pass EVOLVE=1 for the day-2 discount_code column.
seed:
	python3 hadoop_seed/generate_legacy_data.py --date $(DATE) --rows $(ROWS) --output-dir $(SEED_DIR) $(if $(EVOLVE),--evolve-schema,)

load-hdfs:
	bash hadoop_seed/load_to_hdfs.sh $(SEED_DIR)

hdfs-ls:
	curl -s "http://localhost:9870/webhdfs/v1/legacy/erp/orders?op=LISTSTATUS" | python3 -m json.tool

# The CLI authenticates with the profile; drop .env's host/token so they can't clash with it.
BUNDLE := cd databricks && env -u DATABRICKS_HOST -u DATABRICKS_TOKEN databricks bundle

bundle-validate:
	$(BUNDLE) validate -t dev --profile $(DATABRICKS_PROFILE)

bundle-deploy:
	$(BUNDLE) deploy -t dev --profile $(DATABRICKS_PROFILE)

clean:
	rm -rf $(SEED_DIR) spark-warehouse metastore_db derby.log .pytest_cache
