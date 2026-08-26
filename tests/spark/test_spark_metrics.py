from gds_pipeline.spark_transform import (
    hourly_airline_deltas,
    quality_metrics,
    success_token_rows,
)


def clean_df(spark):
    rows = [
        (
            "event-1",
            "ITARES",
            "20180830",
            19,
            "A,ITARES,20180830,19,19:00:00:000,CA:success;CA:success;MU:success;",
        ),
        (
            "event-2",
            "ITARES",
            "20180830",
            19,
            "B,ITARES,20180830,19,19:01:00:000,CA:success;",
        ),
        (
            "event-3",
            "ITAREQ",
            "20180830",
            19,
            "C,ITAREQ,20180830,19,19:02:00:000,CA:success;",
        ),
    ]
    return spark.createDataFrame(
        rows,
        "event_id string, log_type string, event_date string, event_hour int, raw_line string",
    )


def test_success_token_rows_preserve_repeated_occurrences(spark) -> None:
    rows = success_token_rows(clean_df(spark)).orderBy(
        "event_id", "token_ordinal"
    ).collect()

    assert [(row.event_id, row.airline_code) for row in rows] == [
        ("event-1", "CA"),
        ("event-1", "CA"),
        ("event-1", "MU"),
        ("event-2", "CA"),
    ]


def test_hourly_airline_deltas_separate_record_and_token_counts(spark) -> None:
    rows = {
        row.airline_code: row
        for row in hourly_airline_deltas(clean_df(spark), batch_id=7).collect()
    }

    assert set(rows) == {"CA", "MU"}
    assert rows["CA"].successful_response_records == 2
    assert rows["CA"].success_token_count == 3
    assert rows["MU"].successful_response_records == 1
    assert rows["MU"].success_token_count == 1
    assert rows["CA"].batch_id == 7
    assert rows["CA"].event_date == "20180830"
    assert rows["CA"].event_hour == 19


def test_quality_metrics_reconcile_input_and_failure_reasons(spark) -> None:
    input_df = spark.range(5)
    raw_df = spark.range(4)
    clean = spark.range(3)
    dead = spark.createDataFrame(
        [
            ("envelope", "invalid_json"),
            ("gds", "invalid_hour"),
        ],
        "failure_stage string, failure_reason string",
    )

    rows = {
        (row.metric_name, row.failure_stage, row.failure_reason): row.metric_value
        for row in quality_metrics(
            input_df, raw_df, clean, dead, batch_id=9
        ).collect()
    }

    assert rows[("input_count", None, None)] == 5
    assert rows[("envelope_valid_count", None, None)] == 4
    assert rows[("business_valid_count", None, None)] == 3
    assert rows[("dead_letter_count", "envelope", "invalid_json")] == 1
    assert rows[("dead_letter_count", "gds", "invalid_hour")] == 1
    assert all(row.batch_id == 9 for row in quality_metrics(
        input_df, raw_df, clean, dead, batch_id=9
    ).collect())
