import os
from pathlib import Path
import subprocess

import pytest

from gds_pipeline.mysql_config import MySQLSettings
from gds_pipeline.mysql_repository import MySQLRepository, PublicationLockError
from gds_pipeline.mysql_snapshot import load_snapshot


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_MYSQL_INTEGRATION") != "1",
    reason="set RUN_MYSQL_INTEGRATION=1 to use Docker MySQL",
)

FIXTURE = Path(__file__).parents[1] / "fixtures/mysql_snapshot"
ROOT = Path(__file__).parents[2]


def _settings() -> MySQLSettings:
    return MySQLSettings.from_environment()


def _connect(settings: MySQLSettings):
    import mysql.connector

    return mysql.connector.connect(**settings.connector_arguments())


def _assert_empty_serving(settings: MySQLSettings) -> None:
    connection = _connect(settings)
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM hourly_airline_metrics")
        count = cursor.fetchone()[0]
        if count != 0:
            pytest.fail(
                "refusing destructive integration test because serving table "
                f"contains {count} rows"
            )
    finally:
        cursor.close()
        connection.close()


def _clear_fixture(settings: MySQLSettings) -> None:
    connection = _connect(settings)
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            DELETE FROM hourly_airline_metrics
            WHERE publication_id IN (
              SELECT publication_id FROM metric_publications
              WHERE source_hdfs_root = %s
            )
            """,
            ("hdfs://integration/mysql-publication",),
        )
        cursor.execute("DELETE FROM hourly_airline_metrics_staging")
        cursor.execute(
            "DELETE FROM metric_publications WHERE source_hdfs_root = %s",
            ("hdfs://integration/mysql-publication",),
        )
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def test_real_mysql_publish_repeat_force_validate_and_lock() -> None:
    settings = _settings()
    snapshot = load_snapshot(FIXTURE / "metrics.csv", FIXTURE / "manifest.json")
    repository = MySQLRepository(settings)
    _assert_empty_serving(settings)
    _clear_fixture(settings)
    try:
        first = repository.publish(snapshot)
        assert first.status == "published"
        assert repository.validate_serving(snapshot) == (
            True,
            "serving snapshot matches",
        )

        repeated = repository.publish(snapshot)
        assert repeated.status == "unchanged"
        assert repeated.publication_id == first.publication_id

        forced = repository.publish(snapshot, force=True)
        assert forced.status == "published"
        assert forced.publication_id != first.publication_id
        assert repository.validate_serving(snapshot)[0] is True

        lock_connection = _connect(settings)
        lock_cursor = lock_connection.cursor()
        try:
            lock_cursor.execute(
                "SELECT GET_LOCK(%s, 0)",
                ("gds_hourly_airline_metrics_publish",),
            )
            assert lock_cursor.fetchone() == (1,)
            locked_settings = MySQLSettings(
                host=settings.host,
                port=settings.port,
                database=settings.database,
                username=settings.username,
                password=settings.password,
                lock_timeout=0,
            )
            with pytest.raises(PublicationLockError):
                MySQLRepository(locked_settings).publish(snapshot, force=True)
        finally:
            lock_cursor.execute(
                "SELECT RELEASE_LOCK(%s)",
                ("gds_hourly_airline_metrics_publish",),
            )
            lock_cursor.fetchone()
            lock_cursor.close()
            lock_connection.close()
    finally:
        _clear_fixture(settings)


class _FailingConnection:
    """Delegate to real MySQL but fail once after the serving DELETE."""

    def __init__(self, connection):
        self._connection = connection
        self._failed = False

    def cursor(self):
        return _FailingCursor(self, self._connection.cursor())

    def __getattr__(self, name):
        return getattr(self._connection, name)


class _FailingCursor:
    def __init__(self, owner, cursor):
        self._owner = owner
        self._cursor = cursor

    def execute(self, sql, params=()):
        normalized = " ".join(sql.split())
        if (
            normalized.startswith("INSERT INTO hourly_airline_metrics SELECT")
            and not self._owner._failed
        ):
            self._owner._failed = True
            raise RuntimeError("injected failure after serving delete")
        return self._cursor.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._cursor, name)


def test_real_mysql_mid_transaction_failure_preserves_previous_snapshot() -> None:
    settings = _settings()
    snapshot = load_snapshot(FIXTURE / "metrics.csv", FIXTURE / "manifest.json")
    _assert_empty_serving(settings)
    _clear_fixture(settings)
    try:
        normal = MySQLRepository(settings)
        first = normal.publish(snapshot)
        failing = MySQLRepository(
            settings,
            connection_factory=lambda **_: _FailingConnection(_connect(settings)),
        )

        with pytest.raises(RuntimeError, match="after serving delete"):
            failing.publish(snapshot, force=True)

        assert normal.validate_serving(snapshot)[0] is True
        publications = normal.recent_publications(10)
        assert any(item["status"] == "failed" for item in publications)
        assert any(
            item["status"] == "published"
            and item["publication_id"] == first.publication_id
            for item in publications
        )
    finally:
        _clear_fixture(settings)


@pytest.mark.skipif(
    os.getenv("RUN_MYSQL_RESTART_INTEGRATION") != "1",
    reason="set RUN_MYSQL_RESTART_INTEGRATION=1 to restart Docker MySQL",
)
def test_real_mysql_snapshot_survives_container_restart() -> None:
    settings = _settings()
    snapshot = load_snapshot(FIXTURE / "metrics.csv", FIXTURE / "manifest.json")
    _assert_empty_serving(settings)
    _clear_fixture(settings)
    try:
        MySQLRepository(settings).publish(snapshot)
        subprocess.run(
            ["bash", "scripts/mysql-down.sh"], cwd=ROOT, check=True
        )
        subprocess.run(
            ["bash", "scripts/mysql-up.sh"], cwd=ROOT, check=True
        )
        assert MySQLRepository(settings).validate_serving(snapshot)[0] is True
    finally:
        _clear_fixture(settings)
