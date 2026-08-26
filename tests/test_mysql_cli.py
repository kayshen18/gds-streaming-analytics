from pathlib import Path

from gds_pipeline import mysql_cli
from gds_pipeline.mysql_repository import PublicationResult
from gds_pipeline.mysql_snapshot import SnapshotValidationError


def test_publish_maps_paths_expectations_and_force(monkeypatch, tmp_path: Path, capsys) -> None:
    csv_path = tmp_path / "metrics.csv"
    manifest_path = tmp_path / "manifest.json"
    csv_path.touch()
    manifest_path.touch()
    captured = {}

    class Repository:
        def publish(self, snapshot, *, force=False):
            captured["snapshot"] = snapshot
            captured["force"] = force
            return PublicationResult("pub-1", "published", 3, 7, 11, "a" * 64)

    monkeypatch.setattr(mysql_cli, "load_snapshot", lambda *args, **kwargs: captured.update(load=(args, kwargs)) or "snapshot")
    monkeypatch.setattr(mysql_cli, "_repository", lambda: Repository())

    code = mysql_cli.main([
        "publish", "--csv", str(csv_path), "--manifest", str(manifest_path),
        "--expected-rows", "3", "--expected-responses", "7",
        "--expected-tokens", "11", "--expected-sha256", "a" * 64, "--force",
    ])

    assert code == 0
    assert captured["force"] is True
    expectations = captured["load"][1]["expectations"]
    assert (expectations.row_count, expectations.success_token_count) == (3, 11)
    output = capsys.readouterr().out
    assert "status=published" in output and "publication_id=pub-1" in output


def test_validate_returns_mismatch_exit_code(monkeypatch, tmp_path: Path, capsys) -> None:
    csv_path = tmp_path / "metrics.csv"
    manifest_path = tmp_path / "manifest.json"
    csv_path.touch(); manifest_path.touch()

    class Repository:
        def validate_serving(self, snapshot):
            return False, "row_count expected=3 actual=2"

    monkeypatch.setattr(mysql_cli, "load_snapshot", lambda *a, **k: object())
    monkeypatch.setattr(mysql_cli, "_repository", lambda: Repository())

    code = mysql_cli.main(["validate", "--csv", str(csv_path), "--manifest", str(manifest_path)])

    assert code == 6
    assert "row_count expected=3 actual=2" in capsys.readouterr().err


def test_status_prints_sanitized_publications(monkeypatch, capsys) -> None:
    class Repository:
        def recent_publications(self, limit):
            assert limit == 2
            return [
                {
                    "publication_id": "pub-1", "status": "published",
                    "row_count": 3, "metrics_sha256": "a" * 64,
                    "completed_at": "2026-08-13T10:00:00Z",
                }
            ]

    monkeypatch.setattr(mysql_cli, "_repository", lambda: Repository())
    code = mysql_cli.main(["status", "--limit", "2"])

    assert code == 0
    output = capsys.readouterr().out
    assert "pub-1" in output and "password" not in output.lower()


def test_validation_error_is_distinct_and_does_not_connect(monkeypatch, tmp_path: Path) -> None:
    csv_path = tmp_path / "metrics.csv"; csv_path.touch()
    manifest_path = tmp_path / "manifest.json"; manifest_path.touch()
    monkeypatch.setattr(mysql_cli, "load_snapshot", lambda *a, **k: (_ for _ in ()).throw(SnapshotValidationError("bad snapshot")))
    monkeypatch.setattr(mysql_cli, "_repository", lambda: (_ for _ in ()).throw(AssertionError("must not connect")))

    assert mysql_cli.main(["publish", "--csv", str(csv_path), "--manifest", str(manifest_path)]) == 3


def test_missing_file_and_database_failure_have_distinct_codes(monkeypatch, tmp_path: Path) -> None:
    missing = tmp_path / "missing.csv"
    assert mysql_cli.main(["publish", "--csv", str(missing), "--manifest", str(missing)]) == 2

    csv_path = tmp_path / "metrics.csv"; csv_path.touch()
    manifest_path = tmp_path / "manifest.json"; manifest_path.touch()
    monkeypatch.setattr(mysql_cli, "load_snapshot", lambda *a, **k: object())
    monkeypatch.setattr(mysql_cli, "_repository", lambda: (_ for _ in ()).throw(RuntimeError("db unavailable")))
    assert mysql_cli.main(["publish", "--csv", str(csv_path), "--manifest", str(manifest_path)]) == 5
