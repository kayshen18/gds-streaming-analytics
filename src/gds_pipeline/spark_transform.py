"""Pure Spark SQL transformations for Kafka envelopes and GDS records."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from .spark_schema import envelope_schema


REQUIRED_ENVELOPE_FIELDS = (
    "schema_version",
    "event_id",
    "source_file",
    "source_file_sha256",
    "source_line_number",
    "raw_line",
    "produced_at",
)


def parse_kafka_envelopes(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split Kafka rows into trusted envelopes and envelope dead letters."""

    decoded = (
        df.withColumn("kafka_key", F.col("key").cast("string"))
        .withColumn("value_text", F.col("value").cast("string"))
        .withColumn(
            "json_object",
            F.from_json("value_text", MapType(StringType(), StringType())),
        )
        .withColumn("envelope", F.from_json("value_text", envelope_schema()))
        .select(
            "kafka_key",
            "value_text",
            "json_object",
            "envelope.*",
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_timestamp"),
        )
    )

    missing_required = F.lit(False)
    for name in REQUIRED_ENVELOPE_FIELDS:
        missing_required = missing_required | F.col(name).isNull()

    expected_event_id = F.sha2(
        F.concat_ws(
            "\n",
            F.lit("1"),
            F.col("source_file_sha256"),
            F.col("source_line_number").cast("string"),
            F.col("raw_line"),
        ),
        256,
    )
    expected_key = F.coalesce(F.col("group_id"), expected_event_id)
    failure_reason = (
        F.when(F.col("json_object").isNull(), F.lit("invalid_json"))
        .when(missing_required, F.lit("missing_required_fields"))
        .when(
            F.col("schema_version") != F.lit(1),
            F.lit("unsupported_schema_version"),
        )
        .when(F.col("event_id") != expected_event_id, F.lit("event_id_mismatch"))
        .when(F.col("kafka_key") != expected_key, F.lit("kafka_key_mismatch"))
    )
    classified = decoded.withColumn("failure_reason", failure_reason)
    trusted_columns = [
        *envelope_schema().fieldNames(),
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
    ]
    valid = classified.filter(F.col("failure_reason").isNull()).select(
        *trusted_columns
    )
    dead = classified.filter(F.col("failure_reason").isNotNull()).select(
        *trusted_columns,
        "value_text",
        F.lit("envelope").alias("failure_stage"),
        "failure_reason",
    )
    return valid, dead


def parse_gds_records(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split trusted envelopes into clean GDS events and business dead letters."""

    parsed = (
        df.withColumn("fields", F.split(F.col("raw_line"), ",", -1))
        .withColumn("field_count", F.size("fields"))
        .withColumn("parsed_group_id", F.trim(F.try_element_at("fields", F.lit(1))))
        .withColumn("log_type", F.trim(F.try_element_at("fields", F.lit(2))))
        .withColumn("event_date", F.trim(F.try_element_at("fields", F.lit(3))))
        .withColumn("event_hour_text", F.trim(F.try_element_at("fields", F.lit(4))))
        .withColumn("event_time", F.trim(F.try_element_at("fields", F.lit(5))))
        .withColumn("event_hour", F.expr("try_cast(event_hour_text as int)"))
        .withColumn(
            "parsed_date",
            F.expr("try_to_timestamp(event_date, 'yyyyMMdd')").cast("date"),
        )
    )

    failure_reason = (
        F.when(F.trim(F.col("raw_line")) == "", F.lit("blank_line"))
        .when(F.col("field_count") < 5, F.lit("too_few_fields"))
        .when(F.col("parsed_group_id") == "", F.lit("missing_group_id"))
        .when(F.col("log_type") == "", F.lit("missing_log_type"))
        .when(F.col("parsed_date").isNull(), F.lit("invalid_date"))
        .when(
            F.col("event_hour").isNull()
            | ~F.col("event_hour").between(0, 23),
            F.lit("invalid_hour"),
        )
        .when(
            ~F.col("log_type").isin("ITARES", "ITAREQ"),
            F.lit("unsupported_log_type"),
        )
    )
    classified = parsed.withColumn("failure_reason", failure_reason)
    metadata_columns = [
        *df.columns,
        "log_type",
        "event_date",
        "event_hour",
        "event_time",
    ]
    clean = (
        classified.filter(F.col("failure_reason").isNull())
        .drop("group_id")
        .withColumnRenamed("parsed_group_id", "group_id")
        .select(*metadata_columns)
    )
    dead = classified.filter(F.col("failure_reason").isNotNull()).select(
        *df.columns,
        F.lit("gds").alias("failure_stage"),
        "failure_reason",
    )
    return clean, dead
