from typing import Protocol

from gds_pipeline.api.database import DatabasePool
from gds_pipeline.api.models import (
    AirlineSummary,
    AirlinesResponse,
)


AIRLINE_COUNT_SQL = """
SELECT COUNT(DISTINCT airline_code)
FROM hourly_airline_metrics
"""


AIRLINE_RANKING_SQL = """
SELECT
    airline_code,
    SUM(successful_response_records)
        AS successful_response_records,
    SUM(success_token_count)
        AS success_token_count
FROM hourly_airline_metrics
GROUP BY airline_code
ORDER BY successful_response_records DESC, airline_code ASC
LIMIT %s
"""


class AirlinesReader(Protocol):
    def fetch_airlines(self, *, limit: int) -> AirlinesResponse:
        ...


class AirlinesRepository:
    def __init__(self, database: DatabasePool) -> None:
        self._database = database

    def fetch_airlines(self, *, limit: int) -> AirlinesResponse:
        with self._database.connection() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(AIRLINE_COUNT_SQL)
                count_row = cursor.fetchone()

                cursor.execute(
                    AIRLINE_RANKING_SQL,
                    (limit,),
                )
                rows = cursor.fetchall()
            finally:
                cursor.close()

        total_airlines = 0 if count_row is None else count_row[0]

        items = [
            AirlineSummary(
                airline_code=row[0],
                successful_response_records=row[1],
                success_token_count=row[2],
            )
            for row in rows
        ]

        return AirlinesResponse(
            total_airlines=total_airlines,
            items=items,
        )
