from pathlib import Path

from gds_pipeline.cli import main


FIXTURE = Path(__file__).parent / "fixtures" / "mixed_records.txt"


def test_missing_input_returns_argument_error(tmp_path: Path) -> None:
    code = main(
        [
            "profile",
            "--input",
            str(tmp_path / "missing.txt"),
            "--output",
            str(tmp_path / "output"),
        ]
    )
    assert code == 2


def test_profile_command_writes_four_artifacts(tmp_path: Path) -> None:
    output = tmp_path / "output"
    code = main(
        ["profile", "--input", str(FIXTURE), "--output", str(output)]
    )
    assert code == 0
    assert len(list(output.iterdir())) == 4


def test_existing_output_requires_overwrite(tmp_path: Path) -> None:
    output = tmp_path / "output"
    args = ["profile", "--input", str(FIXTURE), "--output", str(output)]
    assert main(args) == 0
    assert main(args) == 2
    assert main([*args, "--overwrite"]) == 0


def test_negative_invalid_limit_returns_argument_error(tmp_path: Path) -> None:
    code = main(
        [
            "profile",
            "--input",
            str(FIXTURE),
            "--output",
            str(tmp_path / "output"),
            "--invalid-limit",
            "-1",
        ]
    )
    assert code == 2


def test_invalid_utf8_returns_decoding_error(tmp_path: Path) -> None:
    source = tmp_path / "bad-encoding.txt"
    source.write_bytes(b"\xff\xfe")
    code = main(
        [
            "profile",
            "--input",
            str(source),
            "--output",
            str(tmp_path / "output"),
        ]
    )
    assert code == 3
