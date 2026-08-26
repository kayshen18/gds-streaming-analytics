from fastapi.testclient import TestClient

from gds_pipeline.api.main import create_app
from gds_pipeline.api.models import (
    HeatmapCell,
    HourlyHeatmapResponse,
)


class ReadyDatabase:
    def is_ready(self) -> bool:
        return True


class FakeHeatmapRepository:
    def __init__(self) -> None:
        self.received_limits: list[int] = []

    def fetch_heatmap(
        self,
        *,
        limit: int,
    ) -> HourlyHeatmapResponse:
        self.received_limits.append(limit)

        return HourlyHeatmapResponse(
            airlines=["CZ", "MU"],
            hours=[0, 1],
            cells=[
                HeatmapCell(
                    airline_code="CZ",
                    stat_hour=0,
                    successful_response_records=500,
                    success_token_count=820,
                ),
                HeatmapCell(
                    airline_code="CZ",
                    stat_hour=1,
                    successful_response_records=420,
                    success_token_count=700,
                ),
                HeatmapCell(
                    airline_code="MU",
                    stat_hour=0,
                    successful_response_records=390,
                    success_token_count=650,
                ),
                HeatmapCell(
                    airline_code="MU",
                    stat_hour=1,
                    successful_response_records=360,
                    success_token_count=610,
                ),
            ],
        )


def test_heatmap_returns_ranked_airlines_and_cells() -> None:
    repository = FakeHeatmapRepository()
    app = create_app(
        database=ReadyDatabase(),
        heatmap_repository=repository,
    )
    client = TestClient(app)

    response = client.get(
        "/api/v1/hourly-heatmap?limit=2"
    )

    assert response.status_code == 200
    assert response.json() == {
        "airlines": ["CZ", "MU"],
        "hours": [0, 1],
        "cells": [
            {
                "airline_code": "CZ",
                "stat_hour": 0,
                "successful_response_records": 500,
                "success_token_count": 820,
            },
            {
                "airline_code": "CZ",
                "stat_hour": 1,
                "successful_response_records": 420,
                "success_token_count": 700,
            },
            {
                "airline_code": "MU",
                "stat_hour": 0,
                "successful_response_records": 390,
                "success_token_count": 650,
            },
            {
                "airline_code": "MU",
                "stat_hour": 1,
                "successful_response_records": 360,
                "success_token_count": 610,
            },
        ],
    }
    assert repository.received_limits == [2]


def test_heatmap_rejects_invalid_limit() -> None:
    repository = FakeHeatmapRepository()
    app = create_app(
        database=ReadyDatabase(),
        heatmap_repository=repository,
    )
    client = TestClient(app)

    response = client.get(
        "/api/v1/hourly-heatmap?limit=0"
    )

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "INVALID_REQUEST",
            "message": "Request validation failed",
        }
    }
    assert repository.received_limits == []
