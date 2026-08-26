"""Structured Streaming query construction and micro-batch routing."""

from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from .spark_config import DatasetPaths, SparkPipelineSettings
from .spark_writer import BatchWriteSummary, write_batch_outputs


def build_kafka_stream(
    spark: SparkSession, settings: SparkPipelineSettings
) -> DataFrame:
    """Build the Kafka streaming source without starting a query."""

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", settings.topic)
        .option("startingOffsets", settings.starting_offsets)
        .option("failOnDataLoss", "true")
        .load()
    )


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    settings: SparkPipelineSettings,
    *,
    batch_writer: Callable[
        [DataFrame, int, DatasetPaths], BatchWriteSummary
    ] = write_batch_outputs,
) -> BatchWriteSummary:
    """Persist one Kafka micro-batch while producing all HDFS outputs."""

    persisted = batch_df.persist()
    try:
        return batch_writer(
            persisted, batch_id, DatasetPaths.from_settings(settings)
        )
    finally:
        persisted.unpersist()


def start_query(
    spark: SparkSession,
    settings: SparkPipelineSettings,
    *,
    stream_builder: Callable[
        [SparkSession, SparkPipelineSettings], DataFrame
    ] = build_kafka_stream,
    batch_processor: Callable[
        [DataFrame, int, SparkPipelineSettings], BatchWriteSummary
    ] = process_batch,
):
    """Start the single checkpointed Structured Streaming query."""

    stream = stream_builder(spark, settings)
    writer = (
        stream.writeStream.foreachBatch(
            lambda batch_df, batch_id: batch_processor(
                batch_df, batch_id, settings
            )
        )
        .option(
            "checkpointLocation",
            DatasetPaths.from_settings(settings).checkpoint,
        )
    )
    if settings.trigger == "available-now":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=settings.processing_interval)
    return writer.start()
