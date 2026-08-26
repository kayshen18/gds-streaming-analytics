from datetime import datetime
import json

from gds_pipeline.event import build_event
from gds_pipeline.spark_schema import envelope_schema
from gds_pipeline.spark_transform import parse_gds_records, parse_kafka_envelopes


SOURCE_SHA256 = "a" * 64


def kafka_row(
    raw_line: str,
    *,
    line_number: int = 1,
    key: bytes | None = None,
    payload_changes: dict[str, object] | None = None,
) -> tuple[bytes | None, bytes, str, int, int, datetime]:
    event = build_event(
        source_file="航空预订日志.txt",
        source_sha256=SOURCE_SHA256,
        line_number=line_number,
        raw_line=raw_line,
    )
    payload = json.loads(event.to_json_bytes())
    payload.update(payload_changes or {})
    encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return (
        event.kafka_key() if key is None else key,
        encoded,
        "gds.raw.v1",
        2,
        line_number - 1,
        datetime(2026, 8, 12),
    )


def kafka_df(spark, rows):
    schema = "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
    return spark.createDataFrame(rows, schema=schema)


def test_envelope_schema_contains_the_versioned_contract() -> None:
    assert envelope_schema().fieldNames() == [
        "schema_version",
        "event_id",
        "source_file",
        "source_file_sha256",
        "source_line_number",
        "group_id",
        "raw_line",
        "produced_at",
    ]


def test_parse_kafka_envelopes_accepts_valid_unicode_and_keeps_metadata(spark) -> None:
    raw_line = "VA.P中文,ITARES,20180830,19,19:45:36:257,CA:success"
    valid, dead = parse_kafka_envelopes(kafka_df(spark, [kafka_row(raw_line)]))

    assert dead.count() == 0
    row = valid.first()
    assert row.raw_line == raw_line
    assert row.group_id == "VA.P中文"
    assert row.topic == "gds.raw.v1"
    assert row.partition == 2
    assert row.offset == 0
    assert row.kafka_timestamp == datetime(2026, 8, 12)


def test_parse_kafka_envelopes_classifies_each_failure(spark) -> None:
    good = kafka_row("A,ITAREQ,20180830,19,19:00:00:000")
    invalid_json = (b"A", b"not-json", "gds.raw.v1", 0, 1, good[-1])
    missing = (b"A", b"{}", "gds.raw.v1", 0, 2, good[-1])
    wrong_schema = kafka_row(
        "A,ITAREQ,20180830,19,19:00:00:000",
        line_number=4,
        payload_changes={"schema_version": 2},
    )
    bad_id = kafka_row(
        "A,ITAREQ,20180830,19,19:00:00:000",
        line_number=5,
        payload_changes={"event_id": "0" * 64},
    )
    wrong_key = kafka_row(
        "A,ITAREQ,20180830,19,19:00:00:000",
        line_number=6,
        key=b"WRONG",
    )

    valid, dead = parse_kafka_envelopes(
        kafka_df(
            spark,
            [good, invalid_json, missing, wrong_schema, bad_id, wrong_key],
        )
    )

    assert valid.count() == 1
    failures = {
        (row.failure_stage, row.failure_reason) for row in dead.collect()
    }
    assert failures == {
        ("envelope", "invalid_json"),
        ("envelope", "missing_required_fields"),
        ("envelope", "unsupported_schema_version"),
        ("envelope", "event_id_mismatch"),
        ("envelope", "kafka_key_mismatch"),
    }


def test_parse_gds_records_matches_baseline_reasons_and_valid_fields(spark) -> None:
    lines = [
        "TB.P1780,ITARES,20180830,19,19:45:36:257,,,1,CA:success;",
        "VA.P2241,ITAREQ,20180830,19,19:45:36:647,payload",
        "",
        "A,ITARES,20180830",
        ",ITARES,20180830,19,19:00:00:000",
        "A,,20180830,19,19:00:00:000",
        "A,ITARES,20181340,19,19:00:00:000",
        "A,ITARES,20180830,24,19:00:00:000",
        "A,OTHER,20180830,19,19:00:00:000",
    ]
    raw, envelope_dead = parse_kafka_envelopes(
        kafka_df(
            spark,
            [kafka_row(line, line_number=index) for index, line in enumerate(lines, 1)],
        )
    )
    clean, business_dead = parse_gds_records(raw)

    assert envelope_dead.count() == 0
    assert clean.count() == 2
    first = clean.orderBy("source_line_number").first()
    assert first.group_id == "TB.P1780"
    assert first.log_type == "ITARES"
    assert first.event_date == "20180830"
    assert first.event_hour == 19
    assert first.event_time == "19:45:36:257"
    assert {
        (row.failure_stage, row.failure_reason)
        for row in business_dead.collect()
    } == {
        ("gds", "blank_line"),
        ("gds", "too_few_fields"),
        ("gds", "missing_group_id"),
        ("gds", "missing_log_type"),
        ("gds", "invalid_date"),
        ("gds", "invalid_hour"),
        ("gds", "unsupported_log_type"),
    }
