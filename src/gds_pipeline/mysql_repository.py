"""Transactional complete-snapshot publication into MySQL."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable
from uuid import uuid4

from .mysql_config import MySQLSettings
from .mysql_snapshot import ValidatedSnapshot


LOCK_NAME = "gds_hourly_airline_metrics_publish"


class PublicationError(RuntimeError):
    """Base class for database publication failures."""


class PublicationLockError(PublicationError):
    """Raised when the single-publisher advisory lock cannot be acquired."""


class PublicationVerificationError(PublicationError):
    """Raised when staging values disagree with the validated snapshot."""


@dataclass(frozen=True, slots=True)
class PublicationResult:
    publication_id: str
    status: str
    row_count: int
    successful_response_records: int
    success_token_count: int
    metrics_sha256: str


class MySQLRepository:
    def __init__(
        self,
        settings: MySQLSettings,
        connection_factory: Callable[..., object] | None = None,
    ) -> None:
        self.settings = settings
        self._connection_factory = connection_factory or _default_connection

    def publish(
        self, snapshot: ValidatedSnapshot, *, force: bool = False
    ) -> PublicationResult:
        connection = self._connection_factory(
            **self.settings.connector_arguments()
        )
        cursor = connection.cursor()
        lock_acquired = False
        audit_started = False
        publication_id = str(uuid4())
        try:
            cursor.execute(
                "SELECT GET_LOCK(%s, %s)",
                (LOCK_NAME, self.settings.lock_timeout),
            )
            lock_row = cursor.fetchone()
            if lock_row is None or lock_row[0] != 1:
                raise PublicationLockError("MySQL publication lock timed out")
            lock_acquired = True

            if not force:
                existing = self._find_existing(cursor, snapshot)
                if existing is not None:
                    return self._result(existing, "unchanged", snapshot)

            connection.start_transaction()
            cursor.execute(
                """
                INSERT INTO metric_publications (
                  publication_id, source_hdfs_root, output_version,
                  source_row_count, successful_response_records,
                  success_token_count, metrics_sha256, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    publication_id,
                    snapshot.manifest.source_hdfs_root,
                    snapshot.manifest.output_version,
                    snapshot.row_count,
                    snapshot.successful_response_records,
                    snapshot.success_token_count,
                    snapshot.metrics_sha256,
                    "preparing",
                ),
            )
            audit_started = True
            cursor.execute("DELETE FROM hourly_airline_metrics_staging")
            self._insert_staging(cursor, snapshot, publication_id)
            cursor.execute(
                """
                SELECT COUNT(*),
                       COALESCE(SUM(successful_response_records), 0),
                       COALESCE(SUM(success_token_count), 0)
                FROM hourly_airline_metrics_staging
                WHERE publication_id = %s
                """,
                (publication_id,),
            )
            staged = tuple(cursor.fetchone())
            expected = (
                snapshot.row_count,
                snapshot.successful_response_records,
                snapshot.success_token_count,
            )
            if staged != expected:
                raise PublicationVerificationError(
                    f"staging verification mismatch: expected={expected}, actual={staged}"
                )

            cursor.execute("DELETE FROM hourly_airline_metrics")
            cursor.execute(
                """
                INSERT INTO hourly_airline_metrics
                SELECT stat_date, stat_hour, airline_code,
                       successful_response_records, success_token_count,
                       publication_id, CURRENT_TIMESTAMP(6)
                FROM hourly_airline_metrics_staging
                WHERE publication_id = %s
                """,
                (publication_id,),
            )
            cursor.execute(
                """
                UPDATE metric_publications
                SET status = %s, completed_at = CURRENT_TIMESTAMP(6)
                WHERE publication_id = %s
                """,
                ("published", publication_id),
            )
            connection.commit()
            return self._result(publication_id, "published", snapshot)
        except Exception as exc:
            connection.rollback()
            if lock_acquired and audit_started:
                self._record_failure(
                    connection, cursor, publication_id, snapshot, exc
                )
            raise
        finally:
            if lock_acquired:
                try:
                    cursor.execute("SELECT RELEASE_LOCK(%s)", (LOCK_NAME,))
                    cursor.fetchone()
                except Exception:
                    pass
            cursor.close()
            connection.close()

    def validate_serving(
        self, snapshot: ValidatedSnapshot
    ) -> tuple[bool, str]:
        connection = self._connection_factory(
            **self.settings.connector_arguments()
        )
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*),
                       COALESCE(SUM(successful_response_records), 0),
                       COALESCE(SUM(success_token_count), 0),
                       COUNT(DISTINCT publication_id)
                FROM hourly_airline_metrics
                """
            )
            actual = tuple(cursor.fetchone())
            expected = (
                snapshot.row_count,
                snapshot.successful_response_records,
                snapshot.success_token_count,
                1,
            )
            if actual != expected:
                return False, f"serving mismatch: expected={expected}, actual={actual}"
            cursor.execute(
                """
                SELECT metrics_sha256 FROM metric_publications
                WHERE publication_id = (
                  SELECT MIN(publication_id) FROM hourly_airline_metrics
                ) AND status = 'published'
                """
            )
            row = cursor.fetchone()
            actual_hash = None if row is None else row[0]
            if actual_hash != snapshot.metrics_sha256:
                return False, (
                    "serving hash mismatch: "
                    f"expected={snapshot.metrics_sha256}, actual={actual_hash}"
                )
            return True, "serving snapshot matches"
        finally:
            cursor.close()
            connection.close()

    def recent_publications(self, limit: int = 10) -> list[dict[str, object]]:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        connection = self._connection_factory(
            **self.settings.connector_arguments()
        )
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                SELECT publication_id, status, source_row_count,
                       metrics_sha256, completed_at
                FROM metric_publications
                ORDER BY started_at DESC LIMIT %s
                """,
                (limit,),
            )
            return [
                {
                    "publication_id": row[0],
                    "status": row[1],
                    "row_count": row[2],
                    "metrics_sha256": row[3],
                    "completed_at": (
                        None if row[4] is None else row[4].isoformat()
                    ),
                }
                for row in cursor.fetchall()
            ]
        finally:
            cursor.close()
            connection.close()

    @staticmethod
    def _find_existing(cursor, snapshot: ValidatedSnapshot) -> str | None:
        cursor.execute(
            """
            SELECT publication_id FROM metric_publications
            WHERE source_hdfs_root = %s AND output_version = %s
              AND metrics_sha256 = %s AND status = 'published'
            ORDER BY completed_at DESC LIMIT 1
            """,
            (
                snapshot.manifest.source_hdfs_root,
                snapshot.manifest.output_version,
                snapshot.metrics_sha256,
            ),
        )
        row = cursor.fetchone()
        return None if row is None else row[0]

    def _insert_staging(
        self, cursor, snapshot: ValidatedSnapshot, publication_id: str
    ) -> None:
        sql = """
            INSERT INTO hourly_airline_metrics_staging (
              stat_date, stat_hour, airline_code,
              successful_response_records, success_token_count, publication_id
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = [
            (
                datetime.strptime(row.stat_date, "%Y%m%d").date().isoformat(),
                row.stat_hour,
                row.airline_code,
                row.successful_response_records,
                row.success_token_count,
                publication_id,
            )
            for row in snapshot.rows
        ]
        for start in range(0, len(values), self.settings.batch_size):
            cursor.executemany(sql, values[start : start + self.settings.batch_size])

    @staticmethod
    def _record_failure(
        connection,
        cursor,
        publication_id: str,
        snapshot: ValidatedSnapshot,
        error: Exception,
    ) -> None:
        try:
            connection.start_transaction()
            cursor.execute(
                """
                INSERT INTO metric_publications (
                  publication_id, source_hdfs_root, output_version,
                  source_row_count, successful_response_records,
                  success_token_count, metrics_sha256, status,
                  failure_message, completed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                          CURRENT_TIMESTAMP(6))
                """,
                (
                    publication_id,
                    snapshot.manifest.source_hdfs_root,
                    snapshot.manifest.output_version,
                    snapshot.row_count,
                    snapshot.successful_response_records,
                    snapshot.success_token_count,
                    snapshot.metrics_sha256,
                    "failed",
                    str(error)[:1024],
                ),
            )
            connection.commit()
        except Exception:
            connection.rollback()

    @staticmethod
    def _result(
        publication_id: str, status: str, snapshot: ValidatedSnapshot
    ) -> PublicationResult:
        return PublicationResult(
            publication_id=publication_id,
            status=status,
            row_count=snapshot.row_count,
            successful_response_records=snapshot.successful_response_records,
            success_token_count=snapshot.success_token_count,
            metrics_sha256=snapshot.metrics_sha256,
        )


def _default_connection(**arguments):
    import mysql.connector

    return mysql.connector.connect(**arguments)
