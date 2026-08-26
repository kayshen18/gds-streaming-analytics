from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
VERSIONS = ROOT / "infrastructure/mysql/versions.env"
SCRIPT = ROOT / "scripts/check-mysql-prerequisites.sh"


def test_mysql_image_is_an_exact_84_lts_patch() -> None:
    text = VERSIONS.read_text(encoding="utf-8")

    assert re.search(
        r"^MYSQL_IMAGE=mysql:8\.4\.\d+-oraclelinux9$", text, re.MULTILINE
    )
    assert "latest" not in text
    assert "MYSQL_CONTAINER_PORT=3306" in text


def test_prerequisite_script_is_read_only_and_checks_required_tools() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "docker version" in text
    assert "docker compose version" in text
    assert "ss -ltn" in text
    assert "docker pull" not in text
    assert "docker compose up" not in text


def test_prerequisite_script_supports_safe_port_selection() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "GDS_MYSQL_HOST_PORT" in text
    assert "mysql_host_port=" in text
    assert "3306" in text
    assert "3307" in text
    assert "65535" in text
