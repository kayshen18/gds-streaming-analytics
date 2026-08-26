"""Spark entry point for exporting HDFS metrics for MySQL publication."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession

from gds_pipeline.mysql_export import SourceIdentity, export_hdfs_snapshot
from gds_pipeline.spark_config import DatasetPaths, SparkPipelineSettings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hdfs-root", required=True)
    parser.add_argument("--checkpoint-root", required=True)
    parser.add_argument("--output-version", required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--source-identity", required=True)
    args = parser.parse_args()

    settings = SparkPipelineSettings(
        hdfs_root=args.hdfs_root,
        checkpoint_root=args.checkpoint_root,
        output_version=args.output_version,
    )
    paths = DatasetPaths.from_settings(settings)
    spark = SparkSession.builder.appName("gds-mysql-snapshot-export").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        manifest = export_hdfs_snapshot(
            spark,
            paths,
            args.destination,
            SourceIdentity(args.source_identity, args.output_version),
        )
        print(
            "MYSQL_SNAPSHOT_EXPORT="
            + json.dumps(
                {
                    "destination": str(args.destination),
                    "metrics_sha256": manifest.metrics_sha256,
                    "row_count": manifest.row_count,
                    "success_token_count": manifest.success_token_count,
                    "successful_response_records": (
                        manifest.successful_response_records
                    ),
                },
                sort_keys=True,
            )
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
