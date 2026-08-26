from pathlib import Path

from gds_pipeline.kafka_cli import main
from gds_pipeline.producer import ProductionSummary
from gds_pipeline.verifier import VerificationSummary


def test_produce_command_maps_arguments(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "input.txt"
    source.write_text("VA.P1,ITAREQ,20160601,00,000001\n", encoding="utf-8")
    captured = []

    def fake_produce(settings):
        captured.append(settings)
        return ProductionSummary(100, 100, 0, 100, 0, False, 0.5)

    monkeypatch.setattr("gds_pipeline.kafka_cli.produce_file", fake_produce)

    code = main(
        [
            "produce",
            "--input",
            str(source),
            "--bootstrap-servers",
            "localhost:9092",
            "--topic",
            "gds.raw.v1",
            "--limit",
            "100",
            "--rate",
            "1000",
            "--checkpoint",
            str(tmp_path / "checkpoint.json"),
            "--reset-checkpoint",
        ]
    )

    assert code == 0
    settings = captured[0]
    assert settings.input_path == source.resolve()
    assert settings.bootstrap_servers == "localhost:9092"
    assert settings.topic == "gds.raw.v1"
    assert settings.limit == 100
    assert settings.rate == 1000
    assert settings.checkpoint_path == (tmp_path / "checkpoint.json").resolve()
    assert settings.reset_checkpoint is True


def test_missing_input_returns_argument_error(tmp_path: Path) -> None:
    code = main(
        [
            "produce",
            "--input",
            str(tmp_path / "missing.txt"),
            "--bootstrap-servers",
            "localhost:9092",
            "--topic",
            "gds.raw.v1",
        ]
    )

    assert code == 2


def test_invalid_limit_returns_argument_error(tmp_path: Path) -> None:
    source = tmp_path / "input.txt"
    source.write_text("record\n", encoding="utf-8")

    code = main(
        [
            "produce",
            "--input",
            str(source),
            "--bootstrap-servers",
            "localhost:9092",
            "--topic",
            "gds.raw.v1",
            "--limit",
            "0",
        ]
    )

    assert code == 2


def test_delivery_failure_returns_nonzero(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "input.txt"
    source.write_text("record\n", encoding="utf-8")
    monkeypatch.setattr(
        "gds_pipeline.kafka_cli.produce_file",
        lambda settings: ProductionSummary(3, 2, 1, 1, 0, False, 0.2),
    )

    code = main(
        [
            "produce",
            "--input",
            str(source),
            "--bootstrap-servers",
            "localhost:9092",
            "--topic",
            "gds.raw.v1",
        ]
    )

    assert code == 5


def test_unflushed_messages_return_nonzero(monkeypatch, tmp_path: Path) -> None:
    source = tmp_path / "input.txt"
    source.write_text("record\n", encoding="utf-8")
    monkeypatch.setattr(
        "gds_pipeline.kafka_cli.produce_file",
        lambda settings: ProductionSummary(3, 2, 0, 2, 1, False, 30.0),
    )

    code = main(
        [
            "produce",
            "--input",
            str(source),
            "--bootstrap-servers",
            "localhost:9092",
            "--topic",
            "gds.raw.v1",
        ]
    )

    assert code == 6


def test_verify_command_maps_arguments(monkeypatch, capsys) -> None:
    captured = []

    def fake_verify(settings):
        captured.append(settings)
        return VerificationSummary(100, 100, 0, 0, {0: 47, 1: 30, 2: 23})

    monkeypatch.setattr("gds_pipeline.kafka_cli.verify_topic", fake_verify)

    code = main(
        [
            "verify",
            "--bootstrap-servers",
            "localhost:9092",
            "--topic",
            "gds.raw.v1",
            "--expected-count",
            "100",
            "--idle-timeout",
            "20",
        ]
    )

    assert code == 0
    assert captured[0].expected_count == 100
    assert captured[0].idle_timeout == 20
    assert "partition_counts=0:47,1:30,2:23" in capsys.readouterr().out


def test_verify_count_mismatch_returns_nonzero(monkeypatch) -> None:
    monkeypatch.setattr(
        "gds_pipeline.kafka_cli.verify_topic",
        lambda settings: VerificationSummary(99, 99, 0, 0, {0: 99}),
    )

    code = main(
        [
            "verify",
            "--bootstrap-servers",
            "localhost:9092",
            "--topic",
            "gds.raw.v1",
            "--expected-count",
            "100",
        ]
    )

    assert code == 7
