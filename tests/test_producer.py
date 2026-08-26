import json
from pathlib import Path

import pytest

from gds_pipeline.checkpoint import Checkpoint
from gds_pipeline.kafka_config import ProducerSettings
from gds_pipeline.producer import DeliveryTracker, produce_file


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"bootstrap_servers": ""}, "bootstrap_servers"),
        ({"topic": "  "}, "topic"),
        ({"limit": 0}, "limit"),
        ({"rate": 0}, "rate"),
        ({"flush_timeout": 0}, "flush_timeout"),
    ],
)
def test_producer_settings_reject_invalid_values(
    overrides: dict[str, object], message: str
) -> None:
    values: dict[str, object] = {
        "bootstrap_servers": "localhost:9092",
        "topic": "gds.raw.v1",
        "limit": None,
        "rate": None,
        "flush_timeout": 30.0,
    }
    values.update(overrides)

    with pytest.raises(ValueError, match=message):
        ProducerSettings(**values)


def test_producer_settings_accept_unlimited_run() -> None:
    settings = ProducerSettings(
        bootstrap_servers="localhost:9092",
        topic="gds.raw.v1",
        limit=None,
        rate=None,
        flush_timeout=30.0,
    )

    assert settings.limit is None
    assert settings.rate is None


def test_producer_client_config_enables_strong_acknowledgements() -> None:
    settings = ProducerSettings(
        bootstrap_servers="localhost:9092",
        topic="gds.raw.v1",
    )

    assert settings.to_client_config() == {
        "bootstrap.servers": "localhost:9092",
        "acks": "all",
        "enable.idempotence": True,
    }


def test_out_of_order_successes_only_advance_contiguous_checkpoint() -> None:
    tracker = DeliveryTracker()

    tracker.mark_success(1)
    assert tracker.contiguous_confirmed_line == 1
    tracker.mark_success(3)
    assert tracker.contiguous_confirmed_line == 1
    tracker.mark_success(2)
    assert tracker.contiguous_confirmed_line == 3


def test_failed_line_blocks_checkpoint_advancement() -> None:
    tracker = DeliveryTracker()

    tracker.mark_success(1)
    tracker.mark_failure(2, "broker rejected record")
    tracker.mark_success(3)

    assert tracker.contiguous_confirmed_line == 1
    assert tracker.failures == {2: "broker rejected record"}


class FakeProducer:
    def __init__(
        self,
        *,
        callback_order: list[int] | None = None,
        fail_line: int | None = None,
        queue_full_once: bool = False,
    ) -> None:
        self.records: list[tuple[str, bytes, bytes, object]] = []
        self.callback_order = callback_order
        self.fail_line = fail_line
        self.queue_full_once = queue_full_once
        self._raised_queue_full = False
        self.poll_calls = 0
        self.flush_calls: list[float] = []

    def produce(
        self,
        *,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: object,
    ) -> None:
        if self.queue_full_once and not self._raised_queue_full:
            self._raised_queue_full = True
            raise BufferError("local queue full")
        self.records.append((topic, key, value, on_delivery))

    def poll(self, timeout: float) -> None:
        self.poll_calls += 1

    def flush(self, timeout: float) -> int:
        self.flush_calls.append(timeout)
        order = self.callback_order or list(range(len(self.records)))
        for index in order:
            payload = json.loads(self.records[index][2].decode("utf-8"))
            error = (
                "delivery failed"
                if payload["source_line_number"] == self.fail_line
                else None
            )
            self.records[index][3](error)
        return 0


def write_source(path: Path) -> None:
    path.write_text(
        "VA.P1,ITAREQ,20160601,00,000001\n"
        "VA.P2,ITAREQ,20160601,00,000002\n"
        "VA.P3,ITARES,20160601,00,000003,CA:success\n",
        encoding="utf-8",
    )


def production_settings(tmp_path: Path, **overrides: object) -> ProducerSettings:
    source = tmp_path / "input.txt"
    write_source(source)
    values: dict[str, object] = {
        "bootstrap_servers": "localhost:9092",
        "topic": "gds.raw.v1",
        "input_path": source,
        "checkpoint_path": tmp_path / "checkpoint.json",
        "flush_timeout": 7.5,
    }
    values.update(overrides)
    return ProducerSettings(**values)


def test_produce_file_honors_limit_and_flushes(tmp_path: Path) -> None:
    fake = FakeProducer()
    settings = production_settings(tmp_path, limit=2)

    summary = produce_file(settings, producer=fake)

    assert summary.submitted == 2
    assert summary.acknowledged == 2
    assert summary.failed == 0
    assert fake.flush_calls == [7.5]


def test_produce_file_skips_checkpointed_lines(tmp_path: Path) -> None:
    settings = production_settings(tmp_path)
    source_sha256 = __import__("hashlib").sha256(
        settings.input_path.read_bytes()
    ).hexdigest()
    Checkpoint(
        schema_version=1,
        source_sha256=source_sha256,
        last_contiguous_confirmed_line=1,
        topic=settings.topic,
        updated_at="2026-08-11T10:00:00.000Z",
    ).save_atomic(settings.checkpoint_path)
    fake = FakeProducer()

    summary = produce_file(settings, producer=fake)

    line_numbers = [
        json.loads(record[2].decode("utf-8"))["source_line_number"]
        for record in fake.records
    ]
    assert line_numbers == [2, 3]
    assert summary.last_contiguous_confirmed_line == 3


def test_produce_file_polls_and_retries_when_queue_is_full(
    tmp_path: Path,
) -> None:
    fake = FakeProducer(queue_full_once=True)

    summary = produce_file(production_settings(tmp_path, limit=1), producer=fake)

    assert summary.submitted == 1
    assert fake.poll_calls >= 1


def test_out_of_order_callbacks_persist_only_contiguous_progress(
    tmp_path: Path,
) -> None:
    fake = FakeProducer(callback_order=[0, 2, 1])
    settings = production_settings(tmp_path)

    summary = produce_file(settings, producer=fake)

    assert summary.last_contiguous_confirmed_line == 3
    assert Checkpoint.load(
        settings.checkpoint_path
    ).last_contiguous_confirmed_line == 3


def test_delivery_failure_is_reported_and_blocks_checkpoint(tmp_path: Path) -> None:
    fake = FakeProducer(fail_line=2)
    settings = production_settings(tmp_path)

    summary = produce_file(settings, producer=fake)

    assert summary.submitted == 3
    assert summary.acknowledged == 2
    assert summary.failed == 1
    assert summary.last_contiguous_confirmed_line == 1


def test_produce_file_reports_progress_every_ten_thousand(
    tmp_path: Path,
) -> None:
    settings = production_settings(tmp_path)
    settings.input_path.write_text(
        "VA.P1,ITAREQ,20160601,00,000001\n" * 10_000,
        encoding="utf-8",
    )
    progress_events: list[int] = []

    produce_file(
        settings,
        producer=FakeProducer(),
        progress=progress_events.append,
    )

    assert progress_events == [10_000]
