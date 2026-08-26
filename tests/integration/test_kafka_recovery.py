"""Opt-in tests against a real local Kafka broker."""

import os
from pathlib import Path
import time
from uuid import uuid4

import pytest

if os.getenv("RUN_KAFKA_INTEGRATION") != "1":
    pytest.skip(
        "set RUN_KAFKA_INTEGRATION=1 to use the local Kafka broker",
        allow_module_level=True,
    )

from confluent_kafka.admin import AdminClient, NewTopic

from gds_pipeline.checkpoint import Checkpoint
from gds_pipeline.kafka_config import ProducerSettings
from gds_pipeline.producer import produce_file
from gds_pipeline.verifier import VerifierSettings, verify_topic


pytestmark = [
    pytest.mark.integration,
]

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


@pytest.fixture
def source_file(tmp_path: Path) -> Path:
    path = tmp_path / "integration-input.txt"
    path.write_text(
        "".join(
            f"VA.P{line_number % 11:04d},ITAREQ,20160601,00,{line_number:06d}\n"
            for line_number in range(1, 101)
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture
def kafka_topic() -> str:
    topic = f"gds.integration.{uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    created = admin.create_topics(
        [NewTopic(topic, num_partitions=3, replication_factor=1)]
    )
    created[topic].result(timeout=20)
    _wait_until_topic_ready(admin, topic)
    try:
        yield topic
    finally:
        deleted = admin.delete_topics([topic], operation_timeout=20)
        deleted[topic].result(timeout=20)


def test_one_hundred_record_round_trip(
    source_file: Path, kafka_topic: str, tmp_path: Path
) -> None:
    production = produce_file(
        ProducerSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            input_path=source_file,
            checkpoint_path=tmp_path / "round-trip.json",
            limit=100,
        )
    )

    verification = verify_topic(
        VerifierSettings(
            BOOTSTRAP_SERVERS,
            kafka_topic,
            expected_count=100,
            idle_timeout=10,
        )
    )

    assert production.submitted == 100
    assert production.acknowledged == 100
    assert production.failed == 0
    assert production.remaining_after_flush == 0
    assert verification.total == 100
    assert verification.valid == 100
    assert verification.invalid == 0
    assert verification.duplicate_event_ids == 0
    assert set(verification.partition_counts) == {0, 1, 2}


def test_checkpoint_resume_does_not_lose_source_lines(
    source_file: Path, kafka_topic: str, tmp_path: Path
) -> None:
    checkpoint_path = tmp_path / "resume.json"
    first = produce_file(
        ProducerSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            input_path=source_file,
            checkpoint_path=checkpoint_path,
            limit=40,
        )
    )
    saved_after_first_run = Checkpoint.load(checkpoint_path)

    second = produce_file(
        ProducerSettings(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            topic=kafka_topic,
            input_path=source_file,
            checkpoint_path=checkpoint_path,
            limit=60,
        )
    )
    saved_after_resume = Checkpoint.load(checkpoint_path)
    verification = verify_topic(
        VerifierSettings(
            BOOTSTRAP_SERVERS,
            kafka_topic,
            expected_count=100,
            idle_timeout=10,
        )
    )

    assert first.last_contiguous_confirmed_line == 40
    assert saved_after_first_run.last_contiguous_confirmed_line == 40
    assert second.submitted == 60
    assert second.last_contiguous_confirmed_line == 100
    assert saved_after_resume.last_contiguous_confirmed_line == 100
    assert verification.total == 100
    assert verification.valid == 100
    assert verification.invalid == 0
    assert verification.duplicate_event_ids == 0


def _wait_until_topic_ready(admin: AdminClient, topic: str) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        metadata = admin.list_topics(timeout=5)
        topic_metadata = metadata.topics.get(topic)
        if (
            topic_metadata is not None
            and topic_metadata.error is None
            and len(topic_metadata.partitions) == 3
        ):
            return
        time.sleep(0.2)
    raise TimeoutError(f"Kafka topic did not become ready: {topic}")
