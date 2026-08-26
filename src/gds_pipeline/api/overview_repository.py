from typing import Protocol
from gds_pipeline.api.database import DatabasePool
from gds_pipeline.api.models import OverviewResponse


OVERVIEW_SQL = """
SELECT
    COUNT(*) AS metric_rows,
    COUNT(DISTINCT airline_code) AS airline_count,
    COALESCE(SUM(successful_response_records), 0)
        AS successful_response_records,
    COALESCE(SUM(success_token_count), 0)
        AS success_token_count,
    MIN(stat_date) AS start_date,
    MAX(stat_date) AS end_date,
    MIN(publication_id) AS publication_id
FROM hourly_airline_metrics
"""

class OverviewNotAvailableError(RuntimeError):
    """Raised when no published overview data is available."""

class OverviewReader(Protocol):
    def fetch_overview(self) -> OverviewResponse:
        ...

class OverviewRepository:
    def __init__(self, database: DatabasePool) -> None:
        self._database = database

    def fetch_overview(self) -> OverviewResponse:
        with self._database.connection() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(OVERVIEW_SQL)
                row = cursor.fetchone()
            finally:
                cursor.close()

        if (
            row is None
            or row[0] == 0
            or row[4] is None
            or row[5] is None
            or row[6] is None
        ):
            raise OverviewNotAvailableError(
                "No overview snapshot is available"
            )

        return OverviewResponse(
            metric_rows=row[0],
            airline_count=row[1],
            successful_response_records=row[2],
            success_token_count=row[3],
            start_date=row[4],
            end_date=row[5],
            publication_id=row[6],
        )
