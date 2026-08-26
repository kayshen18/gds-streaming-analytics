"""Spark schemas for versioned Kafka event envelopes."""

from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


def envelope_schema() -> StructType:
    """Return the explicit schema published by the Kafka producer."""

    return StructType(
        [
            StructField("schema_version", IntegerType(), True),
            StructField("event_id", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("source_file_sha256", StringType(), True),
            StructField("source_line_number", LongType(), True),
            StructField("group_id", StringType(), True),
            StructField("raw_line", StringType(), True),
            StructField("produced_at", StringType(), True),
        ]
    )
