"""Independently reconcile one full HDFS run with the offline baseline."""

import csv
import hashlib
import io
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gds_pipeline.spark_config import DatasetPaths, SparkPipelineSettings


EXPECTED = {
    "input_count": 2_563_566,
    "business_valid_count": 2_560_708,
    "dead_letter_count": 2_858,
    "hour_airline_groups": 3_203,
    "successful_response_records": 1_310_068,
    "success_token_count": 2_145_511,
}


def canonical_metrics_csv(rows) -> bytes:
    target = io.StringIO(newline="")
    writer = csv.writer(target, lineterminator="\n")
    writer.writerow(
        [
            "stat_date",
            "stat_hour",
            "airline_code",
            "successful_response_records",
            "successful_booking_tokens",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                str(row.event_date),
                row.event_hour,
                row.airline_code,
                row.successful_response_records,
                row.success_token_count,
            ]
        )
    return target.getvalue().encode("utf-8")


def main(
    hdfs_root: str,
    checkpoint_root: str,
    expected_metrics_sha256: str,
) -> None:
    spark = SparkSession.builder.appName("gds-full-acceptance").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    paths = DatasetPaths.from_settings(
        SparkPipelineSettings(
            hdfs_root=hdfs_root,
            checkpoint_root=checkpoint_root,
            output_version="v1",
        )
    )
    try:
        raw = spark.read.parquet(paths.raw_events)
        clean = spark.read.parquet(paths.clean_events)
        dead = spark.read.parquet(paths.dead_letter)
        metrics = spark.read.parquet(paths.hourly_airline_metrics)
        totals = metrics.agg(
            F.sum("successful_response_records").alias("response_records"),
            F.sum("success_token_count").alias("success_tokens"),
        ).first()
        ordered_rows = metrics.orderBy(
            "event_date", "event_hour", "airline_code"
        ).collect()
        spark_csv = canonical_metrics_csv(ordered_rows)

        actual = {
            "input_count": raw.count(),
            "business_valid_count": clean.count(),
            "dead_letter_count": dead.count(),
            "hour_airline_groups": len(ordered_rows),
            "successful_response_records": totals.response_records,
            "success_token_count": totals.success_tokens,
        }
        summary = {
            "actual": actual,
            "expected": EXPECTED,
            "counts_match": actual == EXPECTED,
            "spark_metrics_sha256": hashlib.sha256(spark_csv).hexdigest(),
            "baseline_metrics_sha256": expected_metrics_sha256,
        }
        summary["metrics_hash_match"] = (
            summary["spark_metrics_sha256"]
            == summary["baseline_metrics_sha256"]
        )
        summary["accepted"] = (
            summary["counts_match"] and summary["metrics_hash_match"]
        )
        print("FULL_ACCEPTANCE=" + json.dumps(summary, sort_keys=True))
        if not summary["accepted"]:
            raise SystemExit(4)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
