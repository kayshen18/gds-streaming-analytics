from dataclasses import dataclass, field

import pytest

from gds_pipeline.mysql_config import MySQLSettings
from gds_pipeline.mysql_repository import (
    MySQLRepository,
    PublicationLockError,
    PublicationVerificationError,
)
from gds_pipeline.mysql_snapshot import (
    MetricRow,
    SnapshotManifest,
    ValidatedSnapshot,
)


def _snapshot(digest: str = "a" * 64) -> ValidatedSnapshot:
    rows = (
        MetricRow("20180830", 0, "CA", 2, 3),
        MetricRow("20180830", 1, "MU", 1, 1),
    )
    manifest = SnapshotManifest(
        1, "hdfs://example/data/gds", "v1", 2, 3, 4, digest
    )
    return ValidatedSnapshot(rows, manifest, 2, 3, 4, digest)


@dataclass
class FakeState:
    serving: list[tuple] = field(default_factory=list)
    staging: list[tuple] = field(default_factory=list)
    publications: list[dict] = field(default_factory=list)
    locked: bool = False
    deny_lock: bool = False
    corrupt_totals: bool = False
    fail_staging_insert: bool = False
    batch_sizes: list[int] = field(default_factory=list)


class FakeConnection:
    def __init__(self, state: FakeState):
        self.state = state
        self.pending = None
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def start_transaction(self):
        self.pending = (
            list(self.state.serving),
            list(self.state.staging),
            [dict(row) for row in self.state.publications],
        )

    def commit(self):
        self.pending = None
        self.commit_count += 1

    def rollback(self):
        if self.pending is not None:
            serving, staging, publications = self.pending
            self.state.serving = serving
            self.state.staging = staging
            self.state.publications = publications
        self.pending = None
        self.rollback_count += 1

    def close(self):
        self.closed = True


class FakeCursor:
    def __init__(self, connection: FakeConnection):
        self.connection = connection
        self.state = connection.state
        self.result = None

    def execute(self, sql, params=()):
        normalized = " ".join(sql.split())
        if "GET_LOCK" in normalized:
            self.state.locked = not self.state.deny_lock
            self.result = (0,) if self.state.deny_lock else (1,)
        elif "RELEASE_LOCK" in normalized:
            self.state.locked = False
            self.result = (1,)
        elif normalized.startswith("SELECT publication_id FROM metric_publications"):
            match = next(
                (
                    row["publication_id"]
                    for row in reversed(self.state.publications)
                    if row["source_hdfs_root"] == params[0]
                    and row["output_version"] == params[1]
                    and row["metrics_sha256"] == params[2]
                    and row["status"] == "published"
                ),
                None,
            )
            self.result = (match,) if match else None
        elif normalized.startswith("INSERT INTO metric_publications"):
            failure_message = params[8] if len(params) == 9 else None
            self.state.publications.append(
                {
                    "publication_id": params[0],
                    "source_hdfs_root": params[1],
                    "output_version": params[2],
                    "metrics_sha256": params[6],
                    "status": params[7],
                    "failure_message": failure_message,
                }
            )
        elif normalized.startswith("DELETE FROM hourly_airline_metrics_staging"):
            self.state.staging.clear()
        elif normalized.startswith("SELECT COUNT(*)"):
            responses = sum(row[3] for row in self.state.staging)
            tokens = sum(row[4] for row in self.state.staging)
            if self.state.corrupt_totals:
                responses += 1
            self.result = (len(self.state.staging), responses, tokens)
        elif normalized.startswith("DELETE FROM hourly_airline_metrics"):
            self.state.serving.clear()
        elif normalized.startswith("INSERT INTO hourly_airline_metrics SELECT"):
            self.state.serving = list(self.state.staging)
        elif normalized.startswith("UPDATE metric_publications"):
            for row in self.state.publications:
                if row["publication_id"] == params[-1]:
                    row["status"] = params[0]
                    if len(params) == 3:
                        row["failure_message"] = params[1]
        else:
            raise AssertionError(f"unexpected SQL: {normalized}")

    def executemany(self, sql, rows):
        assert "hourly_airline_metrics_staging" in sql
        if self.state.fail_staging_insert:
            raise RuntimeError("injected staging insert failure")
        self.state.batch_sizes.append(len(rows))
        self.state.staging.extend(rows)

    def fetchone(self):
        return self.result

    def close(self):
        pass


def _repository(state: FakeState):
    connection = FakeConnection(state)
    settings = MySQLSettings.from_environment(
        {
            "GDS_MYSQL_HOST": "localhost",
            "GDS_MYSQL_HOST_PORT": "3307",
            "MYSQL_DATABASE": "gds_analytics",
            "MYSQL_USER": "gds_app",
            "MYSQL_PASSWORD": "secret",
            "GDS_MYSQL_BATCH_SIZE": "1",
        }
    )
    return MySQLRepository(settings, connection_factory=lambda **_: connection), connection


def test_publish_replaces_serving_snapshot_and_releases_lock() -> None:
    state = FakeState(serving=[("20170101", 0, "OLD", 9, 9, "old")])
    repository, connection = _repository(state)

    result = repository.publish(_snapshot())

    assert result.status == "published"
    assert result.row_count == 2
    assert [(row[0], row[1], row[2], row[3], row[4]) for row in state.serving] == [
        ("2018-08-30", 0, "CA", 2, 3),
        ("2018-08-30", 1, "MU", 1, 1),
    ]
    assert {row[5] for row in state.serving} == {result.publication_id}
    assert state.publications[-1]["status"] == "published"
    # One commit ends the implicit preflight SELECT transaction; the second
    # atomically publishes the serving snapshot.
    assert connection.commit_count == 2
    assert not state.locked
    assert connection.closed


def test_repeat_snapshot_is_unchanged_without_rewriting_serving() -> None:
    state = FakeState(serving=[("2018-08-30", 0, "CA", 2, 3, "existing")])
    state.publications.append(
        {
            "publication_id": "existing",
            "source_hdfs_root": "hdfs://example/data/gds",
            "output_version": "v1",
            "metrics_sha256": "a" * 64,
            "status": "published",
        }
    )
    before = list(state.serving)
    repository, _ = _repository(state)

    result = repository.publish(_snapshot())

    assert result.status == "unchanged"
    assert result.publication_id == "existing"
    assert state.serving == before
    assert not state.locked


def test_verification_failure_rolls_back_previous_serving_snapshot() -> None:
    old = [("2018-08-29", 23, "CA", 8, 8, "old")]
    state = FakeState(serving=list(old), corrupt_totals=True)
    repository, connection = _repository(state)

    with pytest.raises(PublicationVerificationError, match="staging"):
        repository.publish(_snapshot())

    assert state.serving == old
    assert connection.rollback_count == 1
    assert not state.locked
    assert connection.closed


def test_lock_timeout_never_changes_database_state() -> None:
    old = [("2018-08-29", 23, "CA", 8, 8, "old")]
    state = FakeState(serving=list(old), deny_lock=True)
    repository, connection = _repository(state)

    with pytest.raises(PublicationLockError, match="timed out"):
        repository.publish(_snapshot())

    assert state.serving == old
    assert state.staging == []
    assert state.publications == []
    assert connection.commit_count == 0
    assert not state.locked


def test_force_republishes_identical_snapshot_without_adding_counts() -> None:
    state = FakeState(serving=[("2018-08-30", 0, "CA", 2, 3, "existing")])
    state.publications.append(
        {
            "publication_id": "existing",
            "source_hdfs_root": "hdfs://example/data/gds",
            "output_version": "v1",
            "metrics_sha256": "a" * 64,
            "status": "published",
        }
    )
    repository, _ = _repository(state)

    result = repository.publish(_snapshot(), force=True)

    assert result.status == "published"
    assert result.publication_id != "existing"
    assert sum(row[3] for row in state.serving) == 3
    assert sum(row[4] for row in state.serving) == 4
    assert len(state.serving) == 2


def test_staging_inserts_honor_configured_batch_size() -> None:
    state = FakeState()
    repository, _ = _repository(state)

    repository.publish(_snapshot())

    assert state.batch_sizes == [1, 1]


def test_insert_failure_rolls_back_and_records_failed_audit() -> None:
    old = [("2018-08-29", 23, "CA", 8, 8, "old")]
    state = FakeState(serving=list(old), fail_staging_insert=True)
    repository, connection = _repository(state)

    with pytest.raises(RuntimeError, match="injected staging"):
        repository.publish(_snapshot())

    assert state.serving == old
    assert state.publications[-1]["status"] == "failed"
    assert "injected staging insert failure" in state.publications[-1]["failure_message"]
    assert connection.rollback_count == 1
    # Preflight commit plus the independent failed-audit commit.
    assert connection.commit_count == 2
    assert not state.locked
