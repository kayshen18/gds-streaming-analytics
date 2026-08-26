from pathlib import Path

import pytest

from gds_pipeline.api.config import ApiSettings


def test_settings_load_valid_environment() -> None:
    settings = ApiSettings.from_environment(
        {
            "GDS_MYSQL_HOST": "127.0.0.1",
            "GDS_MYSQL_HOST_PORT": "3307",
            "MYSQL_DATABASE": "gds_analytics",
            "GDS_API_MYSQL_USER": "gds_api",
            "GDS_API_MYSQL_PASSWORD": "read-only-secret",
            "GDS_API_MYSQL_POOL_SIZE": "5",
            "GDS_MYSQL_CONNECTION_TIMEOUT": "10",
            "GDS_MYSQL_READ_TIMEOUT": "30",
            "GDS_API_CORS_ORIGINS": "http://localhost:5173",
            "GDS_ACCEPTED_RUN_PATH": "config/accepted-run.json",
        }
    )

    assert settings.mysql_host == "127.0.0.1"
    assert settings.mysql_port == 3307
    assert settings.mysql_database == "gds_analytics"
    assert settings.mysql_username == "gds_api"
    assert settings.mysql_password == "read-only-secret"
    assert settings.mysql_pool_size == 5
    assert settings.connection_timeout == 10
    assert settings.read_timeout == 30
    assert settings.cors_origins == ("http://localhost:5173",)
    assert settings.accepted_run_path == Path(
        "config/accepted-run.json"
    )
    assert "read-only-secret" not in repr(settings)

def valid_environment() -> dict[str, str]:
    return {
        "GDS_MYSQL_HOST": "127.0.0.1",
        "GDS_MYSQL_HOST_PORT": "3307",
        "MYSQL_DATABASE": "gds_analytics",
        "GDS_API_MYSQL_USER": "gds_api",
        "GDS_API_MYSQL_PASSWORD": "read-only-secret",
        "GDS_API_MYSQL_POOL_SIZE": "5",
        "GDS_MYSQL_CONNECTION_TIMEOUT": "10",
        "GDS_MYSQL_READ_TIMEOUT": "30",
        "GDS_API_CORS_ORIGINS": "http://localhost:5173",
        "GDS_ACCEPTED_RUN_PATH": "config/accepted-run.json",
    }

@pytest.mark.parametrize(
    "variable",
    [
        "GDS_MYSQL_HOST",
        "MYSQL_DATABASE",
        "GDS_API_MYSQL_USER",
        "GDS_API_MYSQL_PASSWORD",
    ],
)
def test_settings_reject_blank_required_values(
    variable: str,
) -> None:
    environment = valid_environment()
    environment[variable] = "   "

    with pytest.raises(ValueError):
        ApiSettings.from_environment(environment)


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("GDS_MYSQL_HOST_PORT", "0"),
        ("GDS_MYSQL_HOST_PORT", "65536"),
        ("GDS_MYSQL_HOST_PORT", "not-a-number"),
        ("GDS_API_MYSQL_POOL_SIZE", "0"),
        ("GDS_MYSQL_CONNECTION_TIMEOUT", "0"),
        ("GDS_MYSQL_READ_TIMEOUT", "0"),
    ],
)
def test_settings_reject_invalid_numbers(
    variable: str,
    value: str,
) -> None:
    environment = valid_environment()
    environment[variable] = value

    with pytest.raises(ValueError):
        ApiSettings.from_environment(environment)


def test_settings_reject_wildcard_cors() -> None:
    environment = valid_environment()
    environment["GDS_API_CORS_ORIGINS"] = "*"

    with pytest.raises(ValueError):
        ApiSettings.from_environment(environment)


def test_settings_reject_blank_metadata_path() -> None:
    environment = valid_environment()
    environment["GDS_ACCEPTED_RUN_PATH"] = "   "

    with pytest.raises(ValueError):
        ApiSettings.from_environment(environment)
