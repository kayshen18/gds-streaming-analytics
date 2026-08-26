import pytest

from gds_pipeline.api.config import ApiSettings
from gds_pipeline.api.database import (
    DatabasePool,
    create_database_pool,
)


class FakeCursor:
    def __init__(self) -> None:
        self.closed = False
        self.executed: list[str] = []
        self.fail_on_execute = False

    def execute(self, statement: str) -> None:
        self.executed.append(statement)
        if self.fail_on_execute:
            raise RuntimeError("database unavailable")

    def fetchone(self) -> tuple[int]:
        return (1,)

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self) -> None:
        self.closed = False
        self.cursor_value = FakeCursor()

    def cursor(self) -> FakeCursor:
        return self.cursor_value

    def close(self) -> None:
        self.closed = True


class FakePool:
    def __init__(self) -> None:
        self.connection = FakeConnection()

    def get_connection(self) -> FakeConnection:
        return self.connection


def test_connection_is_returned_after_success() -> None:
    fake_pool = FakePool()
    database = DatabasePool(fake_pool)

    with database.connection() as connection:
        assert connection is fake_pool.connection
        assert connection.closed is False

    assert fake_pool.connection.closed is True


def test_connection_is_returned_after_failure() -> None:
    fake_pool = FakePool()
    database = DatabasePool(fake_pool)

    with pytest.raises(RuntimeError):
        with database.connection():
            raise RuntimeError("query failed")

    assert fake_pool.connection.closed is True


def test_readiness_executes_select_one_and_closes_resources() -> None:
    fake_pool = FakePool()
    database = DatabasePool(fake_pool)

    assert database.is_ready() is True

    cursor = fake_pool.connection.cursor_value
    assert cursor.executed == ["SELECT 1"]
    assert cursor.closed is True
    assert fake_pool.connection.closed is True


def test_readiness_closes_resources_after_query_failure() -> None:
    fake_pool = FakePool()
    fake_pool.connection.cursor_value.fail_on_execute = True
    database = DatabasePool(fake_pool)

    with pytest.raises(RuntimeError):
        database.is_ready()

    assert fake_pool.connection.cursor_value.closed is True
    assert fake_pool.connection.closed is True

def test_create_database_pool_uses_read_only_settings() -> None:
    settings = ApiSettings(
        mysql_host="127.0.0.1",
        mysql_port=3307,
        mysql_database="gds_analytics",
        mysql_username="gds_api",
        mysql_password="read-only-secret",
        mysql_pool_size=5,
        connection_timeout=10,
        read_timeout=30,
    )
    fake_pool = FakePool()
    captured: dict[str, object] = {}

    def fake_factory(**arguments: object) -> FakePool:
        captured.update(arguments)
        return fake_pool

    database = create_database_pool(
        settings,
        pool_factory=fake_factory,
    )

    assert isinstance(database, DatabasePool)
    assert captured == {
        "pool_name": "gds_api",
        "pool_size": 5,
        "pool_reset_session": True,
        "host": "127.0.0.1",
        "port": 3307,
        "database": "gds_analytics",
        "user": "gds_api",
        "password": "read-only-secret",
        "connection_timeout": 10,
        "read_timeout": 30,
        "autocommit": True,
        "charset": "utf8mb4",
        "use_unicode": True,
    }
