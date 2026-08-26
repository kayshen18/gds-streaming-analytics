"""Deterministic merge of per-batch airline metric deltas."""

from dataclasses import dataclass
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .spark_config import DatasetPaths


@dataclass(frozen=True, slots=True)
class MergeSummary:
    output_rows: int


def merge_hourly_airline_metrics(
    spark: SparkSession, paths: DatasetPaths
) -> MergeSummary:
    """Recompute and atomically replace final metrics from all deltas."""

    dimensions = ["event_date", "event_hour", "airline_code"]
    merged = (
        spark.read.parquet(paths.hourly_airline_deltas)
        .groupBy(*dimensions)
        .agg(
            F.sum("successful_response_records")
            .cast("long")
            .alias("successful_response_records"),
            F.sum("success_token_count")
            .cast("long")
            .alias("success_token_count"),
        )
    )
    output_rows = merged.count()
    staging = f"{paths.hourly_airline_metrics}.__staging__{uuid4().hex}"
    filesystem, path_type = _filesystem(spark)
    staging_path = path_type(staging)
    destination_path = path_type(paths.hourly_airline_metrics)
    try:
        merged.write.mode("overwrite").parquet(staging)
        if filesystem.exists(destination_path):
            filesystem.delete(destination_path, True)
        if not filesystem.rename(staging_path, destination_path):
            raise RuntimeError(
                f"failed to publish merged metrics to {paths.hourly_airline_metrics}"
            )
    finally:
        if filesystem.exists(staging_path):
            filesystem.delete(staging_path, True)
    return MergeSummary(output_rows=output_rows)


def _filesystem(spark: SparkSession):
    context = spark.sparkContext
    jvm = context._jvm
    filesystem = jvm.org.apache.hadoop.fs.FileSystem.get(
        context._jsc.hadoopConfiguration()
    )
    return filesystem, jvm.org.apache.hadoop.fs.Path
