from typing import Protocol

from gds_pipeline.api.database import DatabasePool
from gds_pipeline.api.models import PublicationResponse


PUBLICATION_SQL = """
SELECT
    publication_id,
    source_hdfs_root,
    output_version,
    source_row_count,
    successful_response_records,
    success_token_count,
    metrics_sha256,
    status,
    completed_at
FROM metric_publications
WHERE status = %s
ORDER BY completed_at DESC
LIMIT 1
"""


class PublicationNotAvailableError(RuntimeError):
    """Raised when no published snapshot is available."""

class PublicationReader(Protocol):
    def fetch_publication(self) -> PublicationResponse:
        ...


class PublicationRepository:
    def __init__(self, database: DatabasePool) -> None:
        self._database = database

    def fetch_publication(self) -> PublicationResponse:
        with self._database.connection() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    PUBLICATION_SQL,
                    ("published",),
                )
                row = cursor.fetchone()
            finally:
                cursor.close()

        if row is None:
            raise PublicationNotAvailableError(
                "No published snapshot is available"
            )

        return PublicationResponse(
            publication_id=row[0],
            source_hdfs_root=row[1],
            output_version=row[2],
            source_row_count=row[3],
            successful_response_records=row[4],
            success_token_count=row[5],
            metrics_sha256=row[6],
            status=row[7],
            completed_at=row[8],
        )
