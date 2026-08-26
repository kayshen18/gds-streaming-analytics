from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MYSQL = ROOT / "infrastructure/mysql"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_compose_uses_pinned_image_healthcheck_and_safe_default_port() -> None:
    text = _read(MYSQL / "compose.yaml")

    assert "${MYSQL_IMAGE}" in text
    assert '"${GDS_MYSQL_HOST_PORT:-3307}:3306"' in text
    assert "mysqladmin ping" in text
    assert "mysql:latest" not in text
    assert "mem_limit:" in text
    assert "cpus:" in text


def test_compose_uses_persistent_volume_and_existing_project_network() -> None:
    text = _read(MYSQL / "compose.yaml")

    assert "gds-mysql-data:/var/lib/mysql" in text
    assert "name: gds-mysql-data" in text
    assert "name: gds-streaming-network" in text
    assert "external: true" in text


def test_compose_reads_secrets_from_environment_and_sets_server_contract() -> None:
    text = _read(MYSQL / "compose.yaml")

    assert "MYSQL_ROOT_PASSWORD: ${MYSQL_ROOT_PASSWORD:?" in text
    assert "MYSQL_PASSWORD: ${MYSQL_PASSWORD:?" in text
    assert "change-me" not in text
    assert "utf8mb4" in text
    assert "default-time-zone=+00:00" in text


def test_schema_defines_serving_staging_and_publication_tables() -> None:
    text = _read(MYSQL / "init/001-schema.sql")

    assert "CREATE TABLE IF NOT EXISTS hourly_airline_metrics (" in text
    assert "CREATE TABLE IF NOT EXISTS hourly_airline_metrics_staging (" in text
    assert "CREATE TABLE IF NOT EXISTS metric_publications (" in text
    assert "PRIMARY KEY (stat_date, stat_hour, airline_code)" in text
    assert "CHECK (stat_hour BETWEEN 0 AND 23)" in text
    assert "ENGINE=InnoDB" in text


def test_real_environment_is_ignored_and_example_contains_required_keys() -> None:
    ignore = _read(ROOT / ".gitignore")
    example = _read(MYSQL / ".env.example")

    assert "infrastructure/mysql/.env" in ignore
    assert "MYSQL_ROOT_PASSWORD=" in example
    assert "MYSQL_PASSWORD=" in example


def test_lifecycle_scripts_preserve_data_unless_reset_is_confirmed() -> None:
    down = _read(ROOT / "scripts/mysql-down.sh")
    reset = _read(ROOT / "scripts/mysql-reset.sh")

    assert " down" in down
    assert "--volumes" not in down
    assert '[[ "${1:-}" != "--confirm" ]]' in reset
    assert "gds-mysql-data" in reset
    assert "docker volume rm" in reset
