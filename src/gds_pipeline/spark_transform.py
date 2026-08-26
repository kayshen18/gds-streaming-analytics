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


def success_token_rows(clean_df: DataFrame) -> DataFrame:
    """Emit one row for every airline success token in an ITARES event."""

    token_pattern = r"(?<![A-Z0-9])([A-Z0-9]{2}):success(?![A-Za-z])"
    response_rows = clean_df.filter(F.col("log_type") == "ITARES").withColumn(
        "success_tokens",
        F.regexp_extract_all(F.col("raw_line"), F.lit(token_pattern), F.lit(1)),
    )
    return response_rows.select(
        *[column for column in clean_df.columns],
        F.posexplode("success_tokens").alias("token_ordinal", "airline_code"),
    )


def hourly_airline_deltas(clean_df: DataFrame, batch_id: int) -> DataFrame:
    """Compute the two approved hourly airline metrics for one batch."""

    tokens = success_token_rows(clean_df)
    dimensions = ["event_date", "event_hour", "airline_code"]
    token_counts = tokens.groupBy(*dimensions).agg(
        F.count(F.lit(1)).cast("long").alias("success_token_count")
    )
    response_counts = (
        tokens.select(*dimensions, "event_id")
        .dropDuplicates([*dimensions, "event_id"])
        .groupBy(*dimensions)
        .agg(
            F.count(F.lit(1))
            .cast("long")
            .alias("successful_response_records")
        )
    )
    return response_counts.join(token_counts, dimensions, "inner").select(
        F.lit(batch_id).cast("long").alias("batch_id"),
        *dimensions,
        "successful_response_records",
        "success_token_count",
    )


def quality_metrics(
    input_df: DataFrame,
    raw_df: DataFrame,
    clean_df: DataFrame,
    dead_df: DataFrame,
    batch_id: int,
) -> DataFrame:
    """Produce auditable batch counts and counts by dead-letter reason."""

    spark = input_df.sparkSession
    summary_schema = (
        "batch_id long, metric_name string, failure_stage string, "
        "failure_reason string, metric_value long"
    )
    summary_rows = spark.createDataFrame(
        [
            (batch_id, "input_count", None, None, input_df.count()),
            (batch_id, "envelope_valid_count", None, None, raw_df.count()),
            (batch_id, "business_valid_count", None, None, clean_df.count()),
        ],
        schema=summary_schema,
    )
    dead_counts = dead_df.groupBy("failure_stage", "failure_reason").agg(
        F.count(F.lit(1)).cast("long").alias("metric_value")
    ).select(
        F.lit(batch_id).cast("long").alias("batch_id"),
        F.lit("dead_letter_count").alias("metric_name"),
        "failure_stage",
        "failure_reason",
        "metric_value",
    )
    return summary_rows.unionByName(dead_counts)
