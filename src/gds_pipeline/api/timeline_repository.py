from typing import Protocol

from gds_pipeline.api.database import DatabasePool
from gds_pipeline.api.models import (
    TimelinePoint,
    TimelineResponse,
)


TIMELINE_SQL = """
SELECT
    stat_date,
    stat_hour,
    SUM(successful_response_records)
        AS successful_response_records,
    SUM(success_token_count)
        AS success_token_count
FROM hourly_airline_metrics
GROUP BY stat_date, stat_hour
ORDER BY stat_date, stat_hour
"""


class TimelineReader(Protocol):
    def fetch_timeline(self) -> TimelineResponse:
        ...


class TimelineRepository:
    def __init__(self, database: DatabasePool) -> None:
        self._database = database

    def fetch_timeline(self) -> TimelineResponse:
        with self._database.connection() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(TIMELINE_SQL)
                rows = cursor.fetchall()
            finally:
                cursor.close()

        items = [
            TimelinePoint(
                stat_date=row[0],
                stat_hour=row[1],
                successful_response_records=row[2],
                success_token_count=row[3],
            )
            for row in rows
        ]

        return TimelineResponse(items=items)
