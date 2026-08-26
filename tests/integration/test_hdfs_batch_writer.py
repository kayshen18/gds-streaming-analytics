import os
from pathlib import Path
import subprocess

import pytest


pytestmark = pytest.mark.spark_hdfs_integration
ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.skipif(
    os.getenv("RUN_SPARK_HDFS_INTEGRATION") != "1",
    reason="set RUN_SPARK_HDFS_INTEGRATION=1 to use Docker Spark and HDFS",
)
def test_same_batch_retry_is_idempotent_on_real_hdfs() -> None:
    result = subprocess.run(
        [
            "docker",
            "exec",
            "-e",
            "PYTHONPATH=/opt/gds-app/src",
            "gds-spark-submit",
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            "/opt/gds-app/tests/integration/hdfs_batch_writer_runner.py",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, (
        f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
    )
    assert "hdfs_batch_writer_retry=passed" in result.stdout

