import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# The project modules are scripts, not packages; make them importable like their runtimes do.
for path in (ROOT / "hadoop_seed", ROOT / "airflow" / "plugins", ROOT / "databricks" / "src"):
    sys.path.insert(0, str(path))

# Spark workers must use the same interpreter as the driver (the 3.11 venv), not the system python3.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# collect() renders TIMESTAMPs as naive datetimes in the local zone; pin it to UTC like the jobs.
os.environ["TZ"] = "UTC"
import time  # noqa: E402

time.tzset()
