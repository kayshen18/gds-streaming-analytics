import json
from dataclasses import dataclass

import pytest

from gds_pipeline.event import build_event
from gds_pipeline.verifier import (
    MessageValidationError,
    VerificationAccumulator,
    VerifierSettings,
    verify_topic,
    validate_message,
)


SOURCE_SHA256 = "a" * 64


def valid_message(line_number: int = 1) -> tuple[bytes, bytes]:
    event = build_event(
        source_file="input.txt",
        source_sha256=SOURCE_SHA256,
        line_number=line_number,
        raw_line=f"VA.P{line_number},ITAREQ,20160601,00,000001",
    )
    return event.kafka_key(), event.to_json_bytes()


def mutate_payload(value: bytes, **changes: object) -> bytes:
    payload = json.loads(value.decode("utf-8"))
    payload.update(changes)
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def test_validate_message_accepts_valid_envelope() -> None:
    key, value = valid_message()

    validated = validate_message(key, value, partition=2, offset=7)

    assert validated.event_id == json.loads(value)["event_id"]
    assert validated.partition == 2
    assert validated.offset == 7


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (b"\xff", "UTF-8"),
        (b"not-json", "JSON"),
        (b"{}", "missing fields"),
    ],
)
def test_validate_message_rejects_malformed_value(
    value: bytes, message: str
) -> None:
    with pytest.raises(MessageValidationError, match=message):
        validate_message(b"key", value, partition=0, offset=0)


def test_validate_message_rejects_wrong_schema_version() -> None:
    key, value = valid_message()

    with pytest.raises(MessageValidationError, match="schema_version"):
        validate_message(
            key,
            mutate_payload(value, schema_version=2),
            partition=0,
            offset=0,
        )


def test_validate_message_rejects_bad_event_id() -> None:
    key, value = valid_message()

    with pytest.raises(MessageValidationError, match="event_id"):
        validate_message(
            key,
            mutate_payload(value, event_id="0" * 64),
            partition=0,
            offset=0,
        )


def test_validate_message_rejects_wrong_kafka_key() -> None:
    _, value = valid_message()

    with pytest.raises(MessageValidationError, match="Kafka key"):
        validate_message(b"wrong-key", value, partition=0, offset=0)


def test_accumulator_counts_duplicates_invalid_and_partitions() -> None:
    accumulator = VerificationAccumulator()
    key1, value1 = valid_message(1)
    key2, value2 = valid_message(2)

    accumulator.add(key1, value1, partition=0, offset=0)
    accumulator.add(key2, value2, partition=2, offset=0)
    accumulator.add(key1, value1, partition=0, offset=1)
    accumulator.add(b"wrong", value2, partition=1, offset=0)
    summary = accumulator.summary()

    assert summary.total == 4
    assert summary.valid == 3
    assert summary.invalid == 1
    assert summary.duplicate_event_ids == 1
    assert summary.partition_counts == {0: 2, 1: 1, 2: 1}


@dataclass
class FakeMessage:
    message_key: bytes
    message_value: bytes
    message_partition: int
    message_offset: int
    message_error: object = None

    def key(self):
        return self.message_key

    def value(self):
        return self.message_value

    def partition(self):
        return self.message_partition

    def offset(self):
        return self.message_offset

    def error(self):
        return self.message_error


class FakeConsumer:
    def __init__(self, messages: list[FakeMessage | None]) -> None:
        self.messages = iter(messages)
        self.subscriptions: list[list[str]] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions.append(topics)

    def poll(self, timeout: float):
        return next(self.messages, None)

    def close(self) -> None:
        self.closed = True


def test_verifier_settings_use_earliest_without_auto_commit() -> None:
    settings = VerifierSettings("localhost:9092", "gds.raw.v1")

    config = settings.to_client_config(group_id="verification-run")

    assert config["auto.offset.reset"] == "earliest"
    assert config["enable.auto.commit"] is False
    assert config["group.id"] == "verification-run"


def test_verify_topic_consumes_expected_count_and_closes() -> None:
    key1, value1 = valid_message(1)
    key2, value2 = valid_message(2)
    consumer = FakeConsumer(
        [
            FakeMessage(key1, value1, 0, 0),
            FakeMessage(key2, value2, 2, 0),
        ]
    )
    settings = VerifierSettings(
        "localhost:9092", "gds.raw.v1", expected_count=2, idle_timeout=1
    )

    summary = verify_topic(settings, consumer=consumer)

    assert summary.total == 2
    assert summary.valid == 2
    assert consumer.subscriptions == [["gds.raw.v1"]]
    assert consumer.closed is True


def test_verify_topic_stops_after_idle_timeout(monkeypatch) -> None:
    moments = iter([0.0, 0.4, 1.1])
    monkeypatch.setattr("gds_pipeline.verifier.time.monotonic", lambda: next(moments))
    consumer = FakeConsumer([None, None])
    settings = VerifierSettings(
        "localhost:9092", "gds.raw.v1", idle_timeout=1
    )

    summary = verify_topic(settings, consumer=consumer)

    assert summary.total == 0
    assert consumer.closed is True
