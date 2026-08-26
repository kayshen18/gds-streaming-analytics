from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "live-demo-refresh.sh"


def test_live_demo_refresh_script_has_safe_pipeline_contract() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert "set -euo pipefail" in content
    assert "gds-kafka simulate" in content
    assert "spark-run-available.sh" in content
    assert "hdfs-validate-results.sh" in content
    assert "mysql-export-hdfs-snapshot.sh" in content
    assert "mysql-publish.sh" in content
    assert "mysql-validate.sh" in content


def test_live_demo_refresh_script_supports_bounded_iterations() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert "--iterations" in content
    assert "--interval" in content
    assert "--events-per-cycle" in content
    assert "--rate" in content
    assert "sleep" in content

def run_script(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(SCRIPT), *arguments],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def test_live_demo_refresh_help_succeeds_without_running_pipeline() -> None:
    result = run_script("--help")

    assert result.returncode == 0
    assert "--events-per-cycle" in result.stdout
    assert "Generating" not in result.stdout


def test_live_demo_refresh_rejects_invalid_iterations() -> None:
    result = run_script("--iterations", "0")

    assert result.returncode == 2
    assert "--iterations must be a positive integer" in result.stderr
    assert "Generating" not in result.stdout


def test_live_demo_refresh_rejects_missing_option_value() -> None:
    result = run_script("--rate")

    assert result.returncode == 2
    assert "missing value for --rate" in result.stderr
    assert "Generating" not in result.stdout
