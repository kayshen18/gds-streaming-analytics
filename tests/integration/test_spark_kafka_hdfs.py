import os
from pathlib import Path

import pytest


pytestmark = pytest.mark.spark_pipeline_integration
ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.skipif(
    os.getenv("RUN_SPARK_PIPELINE_INTEGRATION") != "1",
    reason="set RUN_SPARK_PIPELINE_INTEGRATION=1 to use Kafka, Spark, and HDFS",
)
def test_one_hundred_records_reconcile_from_kafka_to_hdfs() -> None:
    from tests.integration.spark_pipeline_harness import run_pipeline_case

    summary = run_pipeline_case(ROOT)

    assert summary["input_count"] == 100
    assert summary["envelope_valid_count"] == 90
    assert summary["business_valid_count"] == 80
    assert summary["dead_letter_count"] == 20
    assert summary["airline_metrics"] == {
        "CA": {
            "successful_response_records": 60,
            "success_token_count": 70,
        },
        "MU": {
            "successful_response_records": 10,
            "success_token_count": 10,
        },
    }


@pytest.mark.skipif(
    os.getenv("RUN_SPARK_PIPELINE_INTEGRATION") != "1",
    reason="set RUN_SPARK_PIPELINE_INTEGRATION=1 to use Kafka, Spark, and HDFS",
)
def test_checkpoint_resume_processes_only_new_kafka_offsets() -> None:
    from tests.integration.spark_pipeline_harness import run_recovery_case

    recovery = run_recovery_case(ROOT)

    assert recovery["first_run_input_count"] == 40
    assert recovery["second_run_input_count"] == 100
    assert recovery["raw_plus_envelope_dead_count"] == 100
    assert recovery["clean_plus_business_dead_count"] == 90
    assert recovery["unique_kafka_locations"] == 100
    assert recovery["airline_metrics"] == {
        "CA": {
            "successful_response_records": 60,
            "success_token_count": 70,
        },
        "MU": {
            "successful_response_records": 10,
            "success_token_count": 10,
        },
    }
