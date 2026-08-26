from typing import Protocol

from gds_pipeline.api.database import DatabasePool
from gds_pipeline.api.models import (
    AirlineTimelineResponse,
    TimelinePoint,
)


AIRLINE_TIMELINE_SQL = """
SELECT
    stat_date,
    stat_hour,
    successful_response_records,
    success_token_count
FROM hourly_airline_metrics
WHERE airline_code = %s
ORDER BY stat_date, stat_hour
"""

class AirlineNotFoundError(RuntimeError):
    """Raised when an airline has no published metrics."""

class AirlineTimelineReader(Protocol):
    def fetch_airline_timeline(
        self,
        *,
        airline_code: str,
    ) -> AirlineTimelineResponse:
        ...


class AirlineTimelineRepository:
    def __init__(self, database: DatabasePool) -> None:
        self._database = database

    def fetch_airline_timeline(
        self,
        *,
        airline_code: str,
    ) -> AirlineTimelineResponse:
        with self._database.connection() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    AIRLINE_TIMELINE_SQL,
                    (airline_code,),
                )
                rows = cursor.fetchall()
            finally:
                cursor.close()

        if not rows:
            raise AirlineNotFoundError(
                f"Airline {airline_code} was not found"
            )

        items = [
            TimelinePoint(
                stat_date=row[0],
                stat_hour=row[1],
                successful_response_records=row[2],
                success_token_count=row[3],
            )
            for row in rows
        ]

        return AirlineTimelineResponse(
            airline_code=airline_code,
            items=items,
        )
