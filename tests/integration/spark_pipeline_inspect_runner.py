"""Read isolated HDFS pipeline outputs and emit one machine-readable summary."""

import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gds_pipeline.spark_config import DatasetPaths, SparkPipelineSettings


def main(hdfs_root: str, checkpoint_root: str) -> None:
    spark = SparkSession.builder.appName("spark-pipeline-inspect").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    paths = DatasetPaths.from_settings(
        SparkPipelineSettings(
            hdfs_root=hdfs_root,
            checkpoint_root=checkpoint_root,
            output_version="v1",
        )
    )
    try:
        raw_count = spark.read.parquet(paths.raw_events).count()
        clean_count = spark.read.parquet(paths.clean_events).count()
        dead_count = spark.read.parquet(paths.dead_letter).count()
        quality = spark.read.parquet(paths.quality_metrics)
        input_count = quality.filter(F.col("metric_name") == "input_count").agg(
            F.sum("metric_value")
        ).first()[0]
        metrics = spark.read.parquet(paths.hourly_airline_metrics)
        airline_metrics = {
            row.airline_code: {
                "successful_response_records": row.successful_response_records,
                "success_token_count": row.success_token_count,
            }
            for row in metrics.collect()
        }
        summary = {
            "input_count": input_count,
            "envelope_valid_count": raw_count,
            "business_valid_count": clean_count,
            "dead_letter_count": dead_count,
            "airline_metrics": airline_metrics,
        }
        print("PIPELINE_SUMMARY=" + json.dumps(summary, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
