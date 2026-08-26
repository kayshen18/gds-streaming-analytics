"""Idempotent, batch-scoped Parquet writes to HDFS."""

from dataclasses import dataclass
import json
from uuid import uuid4

from pyspark.sql import DataFrame

from .spark_config import DatasetPaths
from .spark_transform import (
    hourly_airline_deltas,
    parse_gds_records,
    parse_kafka_envelopes,
    quality_metrics,
)


@dataclass(frozen=True, slots=True)
class BatchWriteSummary:
    batch_id: int
    input_count: int
    envelope_valid_count: int
    business_valid_count: int
    dead_letter_count: int


def write_batch_outputs(
    batch_df: DataFrame, batch_id: int, paths: DatasetPaths
) -> BatchWriteSummary:
    """Transform and atomically replace every output for one micro-batch."""

    if batch_id < 0:
        raise ValueError("batch_id must not be negative")

    raw_df, envelope_dead = parse_kafka_envelopes(batch_df)
    clean_df, business_dead = parse_gds_records(raw_df)
    dead_df = envelope_dead.unionByName(business_dead, allowMissingColumns=True)
    quality_df = quality_metrics(
        batch_df, raw_df, clean_df, dead_df, batch_id=batch_id
    )
    deltas_df = hourly_airline_deltas(clean_df, batch_id=batch_id)

    counts = BatchWriteSummary(
        batch_id=batch_id,
        input_count=batch_df.count(),
        envelope_valid_count=raw_df.count(),
        business_valid_count=clean_df.count(),
        dead_letter_count=dead_df.count(),
    )
    datasets = (
        (paths.raw_events, raw_df),
        (paths.clean_events, clean_df),
        (paths.dead_letter, dead_df),
        (paths.quality_metrics, quality_df),
        (paths.hourly_airline_deltas, deltas_df),
    )
    for dataset_root, frame in datasets:
        _replace_batch(frame, dataset_root, batch_id)
    _write_commit_marker(batch_df, paths, counts)
    return counts


def _replace_batch(frame: DataFrame, dataset_root: str, batch_id: int) -> None:
    staging = f"{dataset_root}/_staging/batch_id={batch_id}-{uuid4().hex}"
    destination = f"{dataset_root}/batch_id={batch_id}"
    filesystem, path_type = _filesystem(frame)
    staging_path = path_type(staging)
    destination_path = path_type(destination)
    try:
        frame.write.mode("overwrite").parquet(staging)
        if filesystem.exists(destination_path):
            filesystem.delete(destination_path, True)
        if not filesystem.rename(staging_path, destination_path):
            raise RuntimeError(
                f"failed to promote staging batch {staging} to {destination}"
            )
    finally:
        if filesystem.exists(staging_path):
            filesystem.delete(staging_path, True)


def _write_commit_marker(
    batch_df: DataFrame,
    paths: DatasetPaths,
    summary: BatchWriteSummary,
) -> None:
    version_root = paths.raw_events.rsplit("/", 1)[0]
    marker_uri = f"{version_root}/_commits/batch_id={summary.batch_id}.json"
    temporary_uri = f"{marker_uri}.tmp-{uuid4().hex}"
    filesystem, path_type = _filesystem(batch_df)
    temporary_path = path_type(temporary_uri)
    marker_path = path_type(marker_uri)
    filesystem.mkdirs(marker_path.getParent())
    payload = json.dumps(
        {
            "batch_id": summary.batch_id,
            "input_count": summary.input_count,
            "envelope_valid_count": summary.envelope_valid_count,
            "business_valid_count": summary.business_valid_count,
            "dead_letter_count": summary.dead_letter_count,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    output = filesystem.create(temporary_path, True)
    try:
        output.write(bytearray(payload))
    finally:
        output.close()
    if filesystem.exists(marker_path):
        filesystem.delete(marker_path, False)
    if not filesystem.rename(temporary_path, marker_path):
        filesystem.delete(temporary_path, False)
        raise RuntimeError(f"failed to publish batch commit marker {marker_uri}")


def _filesystem(frame: DataFrame):
    spark_context = frame.sparkSession.sparkContext
    jvm = spark_context._jvm
    filesystem = jvm.org.apache.hadoop.fs.FileSystem.get(
        spark_context._jsc.hadoopConfiguration()
    )
    return filesystem, jvm.org.apache.hadoop.fs.Path
