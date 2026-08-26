"""Independent validation and accounting for Kafka event envelopes."""

from dataclasses import dataclass
import json
import time
from typing import Any
from typing import Protocol
from uuid import uuid4

from .event import SCHEMA_VERSION, event_id_for


REQUIRED_FIELDS = {
    "schema_version",
    "event_id",
    "source_file",
    "source_file_sha256",
    "source_line_number",
    "group_id",
    "raw_line",
    "produced_at",
}


class MessageValidationError(ValueError):
    """One Kafka record violates the versioned event contract."""


@dataclass(frozen=True, slots=True)
class ValidatedMessage:
    event_id: str
    partition: int
    offset: int


@dataclass(frozen=True, slots=True)
class VerificationSummary:
    total: int
    valid: int
    invalid: int
    duplicate_event_ids: int
    partition_counts: dict[int, int]


@dataclass(frozen=True, slots=True)
class VerifierSettings:
    bootstrap_servers: str
    topic: str
    expected_count: int | None = None
    idle_timeout: float = 20.0

    def __post_init__(self) -> None:
        if not self.bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must not be blank")
        if not self.topic.strip():
            raise ValueError("topic must not be blank")
        if self.expected_count is not None and self.expected_count <= 0:
            raise ValueError("expected_count must be greater than zero")
        if self.idle_timeout <= 0:
            raise ValueError("idle_timeout must be greater than zero")

    def to_client_config(self, *, group_id: str) -> dict[str, object]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }


class ConsumerAdapter(Protocol):
    def subscribe(self, topics: list[str]) -> None: ...

    def poll(self, timeout: float) -> object: ...

    def close(self) -> None: ...


def validate_message(
    key: bytes | None,
    value: bytes | None,
    partition: int,
    offset: int,
) -> ValidatedMessage:
    """Validate one raw Kafka record and return its trusted identity."""

    if value is None:
        raise MessageValidationError("message value is missing")
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError as error:
        raise MessageValidationError("value is not valid UTF-8") from error
    try:
        payload: Any = json.loads(text)
    except json.JSONDecodeError as error:
        raise MessageValidationError("value is not valid JSON") from error
    if not isinstance(payload, dict):
        raise MessageValidationError("JSON value must be an object")
    missing = REQUIRED_FIELDS - payload.keys()
    if missing:
        raise MessageValidationError(
            f"missing fields: {', '.join(sorted(missing))}"
        )
    if payload["schema_version"] != SCHEMA_VERSION:
        raise MessageValidationError("unsupported schema_version")
    _validate_types(payload)

    expected_event_id = event_id_for(
        payload["source_file_sha256"],
        payload["source_line_number"],
        payload["raw_line"],
    )
    if payload["event_id"] != expected_event_id:
        raise MessageValidationError("event_id does not match event content")
    expected_key = (payload["group_id"] or expected_event_id).encode("utf-8")
    if key != expected_key:
        raise MessageValidationError("Kafka key does not match event group")
    return ValidatedMessage(expected_event_id, partition, offset)


def _validate_types(payload: dict[str, Any]) -> None:
    text_fields = (
        "event_id",
        "source_file",
        "source_file_sha256",
        "raw_line",
        "produced_at",
    )
    if any(not isinstance(payload[field], str) for field in text_fields):
        raise MessageValidationError("event text field has invalid type")
    if not isinstance(payload["source_line_number"], int):
        raise MessageValidationError("source_line_number has invalid type")
    if payload["group_id"] is not None and not isinstance(
        payload["group_id"], str
    ):
        raise MessageValidationError("group_id has invalid type")


class VerificationAccumulator:
    """Aggregate validation outcomes without stopping at bad records."""

    def __init__(self) -> None:
        self._total = 0
        self._valid = 0
        self._invalid = 0
        self._duplicate_event_ids = 0
        self._event_ids: set[str] = set()
        self._partition_counts: dict[int, int] = {}

    def add(
        self,
        key: bytes | None,
        value: bytes | None,
        partition: int,
        offset: int,
    ) -> None:
        self._total += 1
        self._partition_counts[partition] = (
            self._partition_counts.get(partition, 0) + 1
        )
        try:
            validated = validate_message(key, value, partition, offset)
        except MessageValidationError:
            self._invalid += 1
            return
        self._valid += 1
        if validated.event_id in self._event_ids:
            self._duplicate_event_ids += 1
        else:
            self._event_ids.add(validated.event_id)

    def summary(self) -> VerificationSummary:
        return VerificationSummary(
            total=self._total,
            valid=self._valid,
            invalid=self._invalid,
            duplicate_event_ids=self._duplicate_event_ids,
            partition_counts=dict(sorted(self._partition_counts.items())),
        )


def verify_topic(
    settings: VerifierSettings,
    *,
    consumer: ConsumerAdapter | None = None,
) -> VerificationSummary:
    """Consume a topic independently and validate every observed record."""

    if consumer is None:
        from confluent_kafka import Consumer

        group_id = f"gds-verifier-{uuid4()}"
        consumer = Consumer(settings.to_client_config(group_id=group_id))
    accumulator = VerificationAccumulator()
    last_message_at = time.monotonic()
    consumer.subscribe([settings.topic])
    try:
        while True:
            message = consumer.poll(min(1.0, settings.idle_timeout))
            now = time.monotonic()
            if message is None:
                if now - last_message_at >= settings.idle_timeout:
                    break
                continue
            error = message.error()
            if error is not None:
                raise RuntimeError(f"Kafka consumer error: {error}")
            last_message_at = now
            accumulator.add(
                message.key(),
                message.value(),
                message.partition(),
                message.offset(),
            )
            summary = accumulator.summary()
            if (
                settings.expected_count is not None
                and summary.total >= settings.expected_count
            ):
                break
    finally:
        consumer.close()
    return accumulator.summary()
