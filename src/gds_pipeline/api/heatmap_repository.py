from typing import Protocol

from gds_pipeline.api.database import DatabasePool
from gds_pipeline.api.models import (
    HeatmapCell,
    HourlyHeatmapResponse,
)


HEATMAP_SQL = """
WITH ranked_airlines AS (
    SELECT
        airline_code,
        ROW_NUMBER() OVER (
            ORDER BY
                SUM(successful_response_records) DESC,
                airline_code ASC
        ) AS airline_rank
    FROM hourly_airline_metrics
    GROUP BY airline_code
),
selected_airlines AS (
    SELECT
        airline_code,
        airline_rank
    FROM ranked_airlines
    WHERE airline_rank <= %s
)
SELECT
    selected_airlines.airline_code,
    hourly_airline_metrics.stat_hour,
    SUM(
        hourly_airline_metrics.successful_response_records
    ) AS successful_response_records,
    SUM(
        hourly_airline_metrics.success_token_count
    ) AS success_token_count
FROM selected_airlines
JOIN hourly_airline_metrics
    ON hourly_airline_metrics.airline_code
        = selected_airlines.airline_code
GROUP BY
    selected_airlines.airline_rank,
    selected_airlines.airline_code,
    hourly_airline_metrics.stat_hour
ORDER BY airline_rank, stat_hour
"""


class HeatmapReader(Protocol):
    def fetch_heatmap(
        self,
        *,
        limit: int,
    ) -> HourlyHeatmapResponse:
        ...


class HeatmapRepository:
    def __init__(self, database: DatabasePool) -> None:
        self._database = database

    def fetch_heatmap(
        self,
        *,
        limit: int,
    ) -> HourlyHeatmapResponse:
        with self._database.connection() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    HEATMAP_SQL,
                    (limit,),
                )
                rows = cursor.fetchall()
            finally:
                cursor.close()

        airlines = list(
            dict.fromkeys(row[0] for row in rows)
        )
        hours = sorted({row[1] for row in rows})

        cells = [
            HeatmapCell(
                airline_code=row[0],
                stat_hour=row[1],
                successful_response_records=row[2],
                success_token_count=row[3],
            )
            for row in rows
        ]

        return HourlyHeatmapResponse(
            airlines=airlines,
            hours=hours,
            cells=cells,
        )
