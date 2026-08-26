from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MYSQL_DIR = ROOT / "infrastructure" / "mysql"
SCRIPT = ROOT / "scripts" / "mysql-create-api-user.sh"


def test_environment_example_defines_api_credentials() -> None:
    example = (MYSQL_DIR / ".env.example").read_text(
        encoding="utf-8"
    )

    assert "GDS_API_MYSQL_USER=gds_api" in example
    assert "GDS_API_MYSQL_PASSWORD=" in example


def test_operator_script_grants_only_required_select_access() -> None:
    script = SCRIPT.read_text(encoding="utf-8")

    assert "CREATE USER IF NOT EXISTS" in script
    assert "GRANT SELECT" in script
    assert "hourly_airline_metrics" in script
    assert "metric_publications" in script
    assert "hourly_airline_metrics_staging" not in script
    assert "GRANT ALL" not in script
    assert "DROP USER" not in script
