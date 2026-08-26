from pathlib import Path

from gds_pipeline.kafka_cli import main
from gds_pipeline.producer import ProductionSummary


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
