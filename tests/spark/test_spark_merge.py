from pathlib import Path

from gds_pipeline.spark_config import DatasetPaths, SparkPipelineSettings
from gds_pipeline.spark_merge import merge_hourly_airline_metrics


def test_merge_sums_all_batch_deltas_and_replaces_final_path(spark, tmp_path: Path) -> None:
    settings = SparkPipelineSettings(
        hdfs_root=tmp_path.as_uri(),
        checkpoint_root=(tmp_path / "checkpoints").as_uri(),
    )
    paths = DatasetPaths.from_settings(settings)
    delta_rows = [
        (0, "20180830", 19, "CA", 2, 3),
        (1, "20180830", 19, "CA", 4, 5),
        (1, "20180830", 19, "MU", 1, 1),
    ]
    deltas = spark.createDataFrame(
        delta_rows,
        "batch_id long, event_date string, event_hour int, airline_code string, successful_response_records long, success_token_count long",
    )
    deltas.write.mode("overwrite").parquet(paths.hourly_airline_deltas)

    first = merge_hourly_airline_metrics(spark, paths)
    second = merge_hourly_airline_metrics(spark, paths)
    rows = {
        row.airline_code: row
        for row in spark.read.parquet(paths.hourly_airline_metrics).collect()
    }

    assert first.output_rows == second.output_rows == 2
    assert rows["CA"].successful_response_records == 6
    assert rows["CA"].success_token_count == 8
    assert rows["MU"].successful_response_records == 1
    assert rows["MU"].success_token_count == 1
