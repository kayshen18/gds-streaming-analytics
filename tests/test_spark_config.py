import pytest

from gds_pipeline.spark_config import DatasetPaths, SparkPipelineSettings


@pytest.mark.parametrize(
    ("field", "value"),
    [("bootstrap_servers", ""), ("topic", "  "), ("hdfs_root", "")],
)
def test_settings_reject_blank_required_values(field: str, value: str) -> None:
    with pytest.raises(ValueError, match=field):
        SparkPipelineSettings(**{field: value})


def test_settings_reject_unsupported_trigger() -> None:
    with pytest.raises(ValueError, match="trigger"):
        SparkPipelineSettings(trigger="once")


def test_processing_time_requires_interval() -> None:
    with pytest.raises(ValueError, match="processing_interval"):
        SparkPipelineSettings(trigger="processing-time")


def test_available_now_rejects_interval() -> None:
    with pytest.raises(ValueError, match="processing_interval"):
        SparkPipelineSettings(
            trigger="available-now", processing_interval="10 seconds"
        )


def test_settings_validate_offsets_and_version() -> None:
    with pytest.raises(ValueError, match="starting_offsets"):
        SparkPipelineSettings(starting_offsets="middle")
    with pytest.raises(ValueError, match="output_version"):
        SparkPipelineSettings(output_version="latest")


def test_processing_time_accepts_interval() -> None:
    settings = SparkPipelineSettings(
        trigger="processing-time", processing_interval="10 seconds"
    )
    assert settings.processing_interval == "10 seconds"


def test_default_dataset_paths_are_exact_and_versioned() -> None:
    paths = DatasetPaths.from_settings(SparkPipelineSettings())

    assert paths.raw_events == "hdfs://hdfs-namenode:8020/data/gds/v1/raw_events"
    assert paths.clean_events == "hdfs://hdfs-namenode:8020/data/gds/v1/clean_events"
    assert paths.dead_letter == "hdfs://hdfs-namenode:8020/data/gds/v1/dead_letter"
    assert paths.quality_metrics == (
        "hdfs://hdfs-namenode:8020/data/gds/v1/quality_metrics"
    )
    assert paths.hourly_airline_deltas == (
        "hdfs://hdfs-namenode:8020/data/gds/v1/hourly_airline_deltas"
    )
    assert paths.hourly_airline_metrics == (
        "hdfs://hdfs-namenode:8020/data/gds/v1/hourly_airline_metrics"
    )
    assert paths.checkpoint == (
        "hdfs://hdfs-namenode:8020/checkpoints/gds/spark-ingestion-v1"
    )


def test_custom_roots_are_normalized_without_double_slashes() -> None:
    settings = SparkPipelineSettings(
        hdfs_root="hdfs://hdfs-namenode:8020/custom/",
        checkpoint_root="hdfs://hdfs-namenode:8020/state/",
        output_version="v2",
    )
    paths = DatasetPaths.from_settings(settings)
    assert paths.raw_events == "hdfs://hdfs-namenode:8020/custom/v2/raw_events"
    assert paths.checkpoint == (
        "hdfs://hdfs-namenode:8020/state/spark-ingestion-v2"
    )
