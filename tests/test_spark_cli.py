from unittest.mock import Mock

import pytest

from gds_pipeline import spark_cli


def test_stream_available_now_builds_settings_waits_and_merges() -> None:
    spark = Mock()
    query = Mock()
    start = Mock(return_value=query)
    merge = Mock()

    exit_code = spark_cli.main(
        [
            "stream",
            "--bootstrap-servers",
            "kafka:29092",
            "--topic",
            "custom.raw.v1",
            "--hdfs-root",
            "hdfs://hdfs-namenode:8020/custom",
            "--checkpoint-root",
            "hdfs://hdfs-namenode:8020/state",
            "--output-version",
            "v2",
            "--starting-offsets",
            "latest",
            "--trigger",
            "available-now",
            "--merge-after",
        ],
        spark_factory=Mock(return_value=spark),
        query_starter=start,
        merger=merge,
    )

    assert exit_code == 0
    settings = start.call_args.args[1]
    assert settings.topic == "custom.raw.v1"
    assert settings.output_version == "v2"
    assert settings.starting_offsets == "latest"
    query.awaitTermination.assert_called_once_with()
    merge.assert_called_once()
    spark.stop.assert_called_once_with()


def test_stream_processing_time_requires_interval(capsys) -> None:
    with pytest.raises(SystemExit) as error:
        spark_cli.main(["stream", "--trigger", "processing-time"])
    assert error.value.code == 2
    assert "--processing-interval" in capsys.readouterr().err


def test_merge_command_invokes_merger() -> None:
    spark = Mock()
    merge = Mock(return_value=Mock(output_rows=3))
    result = spark_cli.main(
        ["merge"], spark_factory=Mock(return_value=spark), merger=merge
    )
    assert result == 0
    merge.assert_called_once()
    spark.stop.assert_called_once_with()


def test_validate_command_returns_nonzero_when_validator_fails() -> None:
    spark = Mock()
    validator = Mock(return_value=False)
    result = spark_cli.main(
        ["validate"],
        spark_factory=Mock(return_value=spark),
        validator=validator,
    )
    assert result == 3
    spark.stop.assert_called_once_with()


def test_parser_exposes_three_subcommands() -> None:
    help_text = spark_cli.build_parser().format_help()
    assert "stream" in help_text
    assert "merge" in help_text
    assert "validate" in help_text
