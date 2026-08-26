import hashlib
import json

from gds_pipeline.event import build_event, event_id_for


SOURCE_SHA256 = "a" * 64


def test_event_id_is_stable_and_uses_versioned_canonical_input() -> None:
    raw_line = "VA.P0001,ITAREQ,20160601,00,000001"
    expected = hashlib.sha256(
        f"1\n{SOURCE_SHA256}\n7\n{raw_line}".encode("utf-8")
    ).hexdigest()

    assert event_id_for(SOURCE_SHA256, 7, raw_line) == expected
    assert event_id_for(SOURCE_SHA256, 7, raw_line) == expected


def test_event_id_changes_with_source_position_or_raw_text() -> None:
    raw_line = "VA.P0001,ITAREQ,20160601,00,000001"
    baseline = event_id_for(SOURCE_SHA256, 7, raw_line)

    assert event_id_for(SOURCE_SHA256, 8, raw_line) != baseline
    assert event_id_for(SOURCE_SHA256, 7, raw_line + ",changed") != baseline


def test_event_json_round_trips_unicode_without_ascii_escaping() -> None:
    event = build_event(
        source_file="航空预订日志.txt",
        source_sha256=SOURCE_SHA256,
        line_number=9,
        raw_line="VA.P中文,ITARES,20160601,00,000002,CA:success",
    )

    encoded = event.to_json_bytes()
    payload = json.loads(encoded.decode("utf-8"))

    assert b"\\u" not in encoded
    assert payload["schema_version"] == 1
    assert payload["source_file"] == "航空预订日志.txt"
    assert payload["group_id"] == "VA.P中文"
    assert payload["raw_line"] == event.raw_line
    assert payload["produced_at"].endswith("Z")


def test_kafka_key_is_group_id_before_first_comma() -> None:
    event = build_event(
        source_file="input.txt",
        source_sha256=SOURCE_SHA256,
        line_number=10,
        raw_line="  VA.P0042  ,ITAREQ,20160601,00,000003",
    )

    assert event.group_id == "VA.P0042"
    assert event.kafka_key() == b"VA.P0042"


def test_comma_only_record_uses_event_id_as_kafka_key() -> None:
    event = build_event(
        source_file="input.txt",
        source_sha256=SOURCE_SHA256,
        line_number=11,
        raw_line=",,,,,,,",
    )

    assert event.group_id is None
    assert event.kafka_key() == event.event_id.encode("ascii")
