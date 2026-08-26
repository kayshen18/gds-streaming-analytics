from unittest.mock import Mock

import pytest

from gds_pipeline.spark_config import DatasetPaths, SparkPipelineSettings
from gds_pipeline.spark_job import build_kafka_stream, process_batch, start_query


class FakeReader:
    def __init__(self):
        self.format_name = None
        self.options = {}
        self.loaded = object()

    def format(self, name):
        self.format_name = name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self):
        return self.loaded


class FakeSpark:
    def __init__(self):
        self.readStream = FakeReader()


def test_build_kafka_stream_uses_settings() -> None:
    spark = FakeSpark()
    settings = SparkPipelineSettings(
        bootstrap_servers="kafka:29092",
        topic="custom.raw.v1",
        starting_offsets="latest",
    )

    result = build_kafka_stream(spark, settings)

    assert result is spark.readStream.loaded
    assert spark.readStream.format_name == "kafka"
    assert spark.readStream.options == {
        "kafka.bootstrap.servers": "kafka:29092",
        "subscribe": "custom.raw.v1",
        "startingOffsets": "latest",
        "failOnDataLoss": "true",
    }


def test_process_batch_persists_writes_and_unpersists() -> None:
    batch_df = Mock()
    persisted = Mock()
    batch_df.persist.return_value = persisted
    writer = Mock(return_value="summary")
    settings = SparkPipelineSettings()

    result = process_batch(batch_df, 7, settings, batch_writer=writer)

    assert result == "summary"
    writer.assert_called_once_with(
        persisted, 7, DatasetPaths.from_settings(settings)
    )
    persisted.unpersist.assert_called_once_with()


def test_process_batch_unpersists_when_writer_fails() -> None:
    batch_df = Mock()
    persisted = Mock()
    batch_df.persist.return_value = persisted
    writer = Mock(side_effect=RuntimeError("write failed"))

    with pytest.raises(RuntimeError, match="write failed"):
        process_batch(
            batch_df, 7, SparkPipelineSettings(), batch_writer=writer
        )

    persisted.unpersist.assert_called_once_with()


class FakeQuery:
    pass


class FakeWriter:
    def __init__(self):
        self.foreach = None
        self.options = {}
        self.trigger_options = None
        self.query = FakeQuery()

    def foreachBatch(self, callback):
        self.foreach = callback
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def trigger(self, **options):
        self.trigger_options = options
        return self

    def start(self):
        return self.query


class FakeStream:
    def __init__(self):
        self.writeStream = FakeWriter()


@pytest.mark.parametrize(
    ("settings", "expected_trigger"),
    [
        (SparkPipelineSettings(), {"availableNow": True}),
        (
            SparkPipelineSettings(
                trigger="processing-time", processing_interval="10 seconds"
            ),
            {"processingTime": "10 seconds"},
        ),
    ],
)
def test_start_query_maps_trigger_and_checkpoint(settings, expected_trigger) -> None:
    stream = FakeStream()
    callback = Mock()

    query = start_query(
        FakeSpark(),
        settings,
        stream_builder=lambda _spark, _settings: stream,
        batch_processor=callback,
    )

    assert query is stream.writeStream.query
    assert stream.writeStream.trigger_options == expected_trigger
    assert stream.writeStream.options["checkpointLocation"] == (
        DatasetPaths.from_settings(settings).checkpoint
    )
    stream.writeStream.foreach("batch", 3)
    callback.assert_called_once_with("batch", 3, settings)

