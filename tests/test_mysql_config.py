import pytest

from gds_pipeline.mysql_config import MySQLSettings


BASE_ENV = {
    "GDS_MYSQL_HOST": "127.0.0.1",
    "GDS_MYSQL_HOST_PORT": "3307",
    "MYSQL_DATABASE": "gds_analytics",
    "MYSQL_USER": "gds_app",
    "MYSQL_PASSWORD": "private-password",
}


def test_settings_load_required_environment_and_hide_password() -> None:
    settings = MySQLSettings.from_environment(BASE_ENV)

    assert settings.host == "127.0.0.1"
    assert settings.port == 3307
    assert settings.database == "gds_analytics"
    assert settings.username == "gds_app"
    assert settings.password == "private-password"
    assert "private-password" not in repr(settings)
    assert settings.connection_timeout == 10
    assert settings.read_timeout == 30
    assert settings.write_timeout == 30
    assert settings.batch_size == 500
    assert settings.lock_timeout == 10


@pytest.mark.parametrize(
    "name",
    ["GDS_MYSQL_HOST", "MYSQL_DATABASE", "MYSQL_USER", "MYSQL_PASSWORD"],
)
def test_settings_reject_missing_or_blank_required_values(name: str) -> None:
    environment = dict(BASE_ENV)
    environment[name] = "  "

    with pytest.raises(ValueError, match=name):
        MySQLSettings.from_environment(environment)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("GDS_MYSQL_HOST_PORT", "0"),
        ("GDS_MYSQL_HOST_PORT", "65536"),
        ("GDS_MYSQL_HOST_PORT", "not-a-number"),
        ("GDS_MYSQL_CONNECTION_TIMEOUT", "0"),
        ("GDS_MYSQL_READ_TIMEOUT", "-1"),
        ("GDS_MYSQL_WRITE_TIMEOUT", "0"),
        ("GDS_MYSQL_BATCH_SIZE", "0"),
        ("GDS_MYSQL_LOCK_TIMEOUT", "-1"),
    ],
)
def test_settings_reject_invalid_numeric_values(name: str, value: str) -> None:
    environment = dict(BASE_ENV)
    environment[name] = value

    with pytest.raises(ValueError, match=name):
        MySQLSettings.from_environment(environment)


def test_settings_accept_explicit_timeouts_and_batch_size() -> None:
    settings = MySQLSettings.from_environment(
        {
            **BASE_ENV,
            "GDS_MYSQL_CONNECTION_TIMEOUT": "7",
            "GDS_MYSQL_READ_TIMEOUT": "11",
            "GDS_MYSQL_WRITE_TIMEOUT": "13",
            "GDS_MYSQL_BATCH_SIZE": "200",
            "GDS_MYSQL_LOCK_TIMEOUT": "0",
        }
    )

    assert settings.connection_timeout == 7
    assert settings.read_timeout == 11
    assert settings.write_timeout == 13
    assert settings.batch_size == 200
    assert settings.lock_timeout == 0


def test_connector_arguments_use_mysql_connector_names() -> None:
    settings = MySQLSettings.from_environment(BASE_ENV)

    assert settings.connector_arguments() == {
        "host": "127.0.0.1",
        "port": 3307,
        "database": "gds_analytics",
        "user": "gds_app",
        "password": "private-password",
        "connection_timeout": 10,
        "read_timeout": 30,
        "write_timeout": 30,
        "autocommit": False,
        "charset": "utf8mb4",
        "use_unicode": True,
    }
