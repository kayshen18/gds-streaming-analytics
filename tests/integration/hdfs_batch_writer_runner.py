"""Real Spark/HDFS assertion runner invoked by the guarded pytest test."""

from datetime import datetime
from uuid import uuid4

from pyspark.sql import SparkSession

from gds_pipeline.event import build_event
from gds_pipeline.spark_config import DatasetPaths, SparkPipelineSettings
from gds_pipeline.spark_writer import write_batch_outputs


def main() -> None:
    spark = SparkSession.builder.appName("hdfs-batch-writer-retry").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    run_id = uuid4().hex
    root = f"hdfs://hdfs-namenode:8020/data/gds-integration/{run_id}"
    checkpoint = f"hdfs://hdfs-namenode:8020/checkpoints/gds-integration/{run_id}"
    settings = SparkPipelineSettings(
        hdfs_root=root,
        checkpoint_root=checkpoint,
        output_version="v1",
    )
    paths = DatasetPaths.from_settings(settings)
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jsc.hadoopConfiguration()
    )
    root_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(root)

    event = build_event(
        source_file="integration.txt",
        source_sha256="a" * 64,
        line_number=1,
        raw_line="A,ITARES,20180830,19,19:00:00:000,CA:success",
    )
    batch_df = spark.createDataFrame(
        [(event.kafka_key(), event.to_json_bytes(), "integration", 0, 0, datetime(2026, 8, 12))],
        "key binary, value binary, topic string, partition int, offset long, timestamp timestamp",
    )

    try:
        first = write_batch_outputs(batch_df, batch_id=7, paths=paths)
        second = write_batch_outputs(batch_df, batch_id=7, paths=paths)
        assert first.batch_id == second.batch_id == 7
        assert spark.read.parquet(f"{paths.raw_events}/batch_id=7").count() == 1
        assert spark.read.parquet(f"{paths.clean_events}/batch_id=7").count() == 1
        assert spark.read.parquet(f"{paths.dead_letter}/batch_id=7").count() == 0
        assert spark.read.parquet(f"{paths.quality_metrics}/batch_id=7").count() >= 3
        assert spark.read.parquet(f"{paths.hourly_airline_deltas}/batch_id=7").count() == 1
        print("hdfs_batch_writer_retry=passed")
    finally:
        fs.delete(root_path, True)
        fs.delete(
            spark.sparkContext._jvm.org.apache.hadoop.fs.Path(checkpoint), True
        )
        spark.stop()


if __name__ == "__main__":
    main()
