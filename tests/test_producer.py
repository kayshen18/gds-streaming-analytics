import pytest

from gds_pipeline.kafka_config import ProducerSettings
from gds_pipeline.producer import DeliveryTracker


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
