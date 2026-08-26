"""Run a processing-time query until one expected wave is committed."""

import sys
import time

from pyspark.sql import SparkSession

from gds_pipeline.spark_config import SparkPipelineSettings
from gds_pipeline.spark_job import start_query


def main(
    topic: str,
    hdfs_root: str,
    checkpoint_root: str,
    expected_rows: int,
) -> None:
    spark = SparkSession.builder.appName("gds-continuous-integration").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    settings = SparkPipelineSettings(
        bootstrap_servers="kafka:29092",
        topic=topic,
        starting_offsets="earliest",
        hdfs_root=hdfs_root,
        checkpoint_root=checkpoint_root,
        output_version="v1",
        trigger="processing-time",
        processing_interval="2 seconds",
    )
    query = start_query(spark, settings)
    deadline = time.monotonic() + 180
    observed_batch_ids: set[int] = set()
    observed_rows = 0
    try:
        while time.monotonic() < deadline:
            if query.exception() is not None:
                raise RuntimeError(str(query.exception()))
            for progress in query.recentProgress:
                batch_id = int(progress["batchId"])
                if batch_id not in observed_batch_ids:
                    observed_batch_ids.add(batch_id)
                    observed_rows += int(progress["numInputRows"])
            if observed_rows >= expected_rows:
                query.stop()
                print(
                    "CONTINUOUS_WAVE_COMPLETE="
                    f"expected={expected_rows},observed={observed_rows}"
                )
                return
            time.sleep(1)
        raise TimeoutError(
            f"continuous query observed {observed_rows}/{expected_rows} rows"
        )
    finally:
        if query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))
