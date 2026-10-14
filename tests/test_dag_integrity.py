"""The DAG imports cleanly and has the task order from the spec."""
import os
import tempfile
from pathlib import Path

import pytest

os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow_home_"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
os.environ.setdefault("DATABRICKS_JOB_ID", "470491837537884")
pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert bag.import_errors == {}
    return bag.dags["legacy_hadoop_to_databricks_lakehouse"]  # get_dag() would query the metadata DB


def test_schedule(dag):
    assert dag is not None
    assert dag.schedule_interval == "@daily"
    assert dag.catchup is False


def test_task_chain(dag):
    order = [
        "verify_hdfs_partition",
        "distcp_hdfs_to_gcs",
        "run_databricks_bronze",
        "run_databricks_silver",
        "run_databricks_gold",
        "audit_row_counts",
    ]
    assert sorted(dag.task_ids) == sorted(order)
    for upstream, downstream in zip(order, order[1:]):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}


@pytest.mark.parametrize(
    "task_id, job_task_key",
    [
        ("run_databricks_bronze", "bronze_ingest"),
        ("run_databricks_silver", "silver_upsert"),
        ("run_databricks_gold", "gold_aggregate"),
    ],
)
def test_databricks_tasks_run_one_task_of_the_bundle_job(dag, task_id, job_task_key):
    task = dag.get_task(task_id)
    assert type(task).__name__ == "DatabricksRunNowOperator"
    assert task.json["job_id"] == os.environ["DATABRICKS_JOB_ID"]
    assert task.json["only"] == [job_task_key]
    assert task.json["job_parameters"] == {"batch_date": "{{ ds }}"}
    # Compute, scripts and retries belong to the bundle job, not the DAG.
    assert not {"tasks", "new_cluster", "existing_cluster_id", "spark_python_task", "environments"} & set(task.json)


def test_job_task_keys_exist_in_the_bundle():
    bundle = (Path(__file__).resolve().parents[1] / "databricks" / "databricks.yml").read_text()
    for key in ("bronze_ingest", "silver_upsert", "gold_aggregate"):
        assert f"task_key: {key}" in bundle
