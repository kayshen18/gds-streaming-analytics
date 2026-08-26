import json

from datetime import datetime, timezone

from gds_pipeline.models import ParseStatus
from gds_pipeline.parser import parse_line
from gds_pipeline.kafka_config import ProducerSettings
from gds_pipeline.simulator import (
    SimulatedRecordGenerator,
    build_simulated_line,
    simulate_to_kafka,
)

OBSERVED_AT = datetime(
    2026,
    8,
    25,
    8,
    14,
    5,
    123000,
    tzinfo=timezone.utc,
)


def test_builds_valid_successful_response_record() -> None:
    line = build_simulated_line(
        sequence=7,
        observed_at=OBSERVED_AT,
        airline_code="CZ",
        log_type="ITARES",
        successful=True,
    )

    record = parse_line(line, 7)

    assert record.parse_status is ParseStatus.VALID
    assert record.group_id == "SIM.P00000007"
    assert record.log_type == "ITARES"
    assert record.event_date == "20260825"
    assert record.event_hour == 8
    assert record.event_time == "08:14:05:123"
    assert record.success_tokens == ("CZ",)


def test_builds_valid_request_record_without_success_tokens() -> None:
    line = build_simulated_line(
        sequence=8,
        observed_at=OBSERVED_AT,
        airline_code="MU",
        log_type="ITAREQ",
        successful=False,
    )

    record = parse_line(line, 8)

    assert record.parse_status is ParseStatus.VALID
    assert record.group_id == "SIM.P00000008"
    assert record.log_type == "ITAREQ"
    assert record.event_date == "20260825"
    assert record.event_hour == 8
    assert record.success_tokens == ()

def test_generator_produces_reproducible_valid_sequence() -> None:
    first = SimulatedRecordGenerator(
        airline_codes=("CZ", "MU", "CA"),
        seed=17,
        clock=lambda: OBSERVED_AT,
    )
    second = SimulatedRecordGenerator(
        airline_codes=("CZ", "MU", "CA"),
        seed=17,
        clock=lambda: OBSERVED_AT,
    )

    first_lines = [first.next_line() for _ in range(20)]
    second_lines = [second.next_line() for _ in range(20)]

    assert first_lines == second_lines

    records = [
        parse_line(line, index)
        for index, line in enumerate(first_lines, start=1)
    ]

    assert all(
        record.parse_status is ParseStatus.VALID
        for record in records
    )
    assert [
        record.group_id
        for record in records
    ] == [
        f"SIM.P{index:08d}"
        for index in range(1, 21)
    ]
    assert all(
        record.event_date == "20260825"
        for record in records
    )
    assert all(
        record.event_hour == 8
        for record in records
    )


def test_generator_rejects_empty_airline_list() -> None:
    try:
        SimulatedRecordGenerator(
            airline_codes=(),
            seed=17,
            clock=lambda: OBSERVED_AT,
        )
    except ValueError as error:
        assert str(error) == (
            "airline_codes must contain at least one code"
        )
    else:
        raise AssertionError("expected ValueError")

class FakeProducer:
    def __init__(self) -> None:
        self.records: list[
            tuple[str, bytes, bytes]
        ] = []
        self.flush_calls: list[float] = []

    def produce(
        self,
        *,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery,
    ) -> None:
        self.records.append((topic, key, value))
        on_delivery(None)

    def poll(self, timeout: float) -> None:
        return None

    def flush(self, timeout: float) -> int:
        self.flush_calls.append(timeout)
        return 0


def test_simulation_publishes_limited_valid_events() -> None:
    producer = FakeProducer()
    generator = SimulatedRecordGenerator(
        airline_codes=("CZ", "MU"),
        seed=23,
        clock=lambda: OBSERVED_AT,
    )
    settings = ProducerSettings(
        bootstrap_servers="localhost:9092",
        topic="gds.raw.v1",
        limit=3,
        flush_timeout=7.5,
    )

    summary = simulate_to_kafka(
        settings,
        generator=generator,
        run_id="unit-test-run",
        producer=producer,
    )

    assert summary.submitted == 3
    assert summary.acknowledged == 3
    assert summary.failed == 0
    assert summary.remaining_after_flush == 0
    assert summary.interrupted is False
    assert producer.flush_calls == [7.5]

    payloads = [
        json.loads(value.decode("utf-8"))
        for _, _, value in producer.records
    ]

    assert [
        payload["source_line_number"]
        for payload in payloads
    ] == [1, 2, 3]
    assert all(
        payload["source_file"]
        == "gds-simulator:unit-test-run"
        for payload in payloads
    )
    assert len({
        payload["event_id"]
        for payload in payloads
    }) == 3
    assert all(
        parse_line(
            payload["raw_line"],
            payload["source_line_number"],
        ).parse_status is ParseStatus.VALID
        for payload in payloads
    )
