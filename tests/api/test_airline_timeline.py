from datetime import date

from fastapi.testclient import TestClient

from gds_pipeline.api.airline_timeline_repository import (
    AirlineNotFoundError,
)
from gds_pipeline.api.main import create_app
from gds_pipeline.api.models import (
    AirlineTimelineResponse,
    TimelinePoint,
)


class ReadyDatabase:
    def is_ready(self) -> bool:
        return True


class FakeAirlineTimelineRepository:
    def __init__(self) -> None:
        self.received_codes: list[str] = []

    def fetch_airline_timeline(
        self,
        *,
        airline_code: str,
    ) -> AirlineTimelineResponse:
        self.received_codes.append(airline_code)
        return AirlineTimelineResponse(
            airline_code=airline_code,
            items=[
                TimelinePoint(
                    stat_date=date(2018, 8, 30),
                    stat_hour=0,
                    successful_response_records=120,
                    success_token_count=205,
                ),
                TimelinePoint(
                    stat_date=date(2018, 8, 30),
                    stat_hour=1,
                    successful_response_records=135,
                    success_token_count=220,
                ),
            ],
        )


class MissingAirlineTimelineRepository:
    def fetch_airline_timeline(
        self,
        *,
        airline_code: str,
    ) -> AirlineTimelineResponse:
        raise AirlineNotFoundError(
            f"Airline {airline_code} was not found"
        )


def test_airline_timeline_normalizes_code_and_returns_points() -> None:
    repository = FakeAirlineTimelineRepository()
    app = create_app(
        database=ReadyDatabase(),
        airline_timeline_repository=repository,
    )
    client = TestClient(app)

    response = client.get(
        "/api/v1/airlines/ca/timeline"
    )

    assert response.status_code == 200
    assert response.json() == {
        "airline_code": "CA",
        "items": [
            {
                "stat_date": "2018-08-30",
                "stat_hour": 0,
                "successful_response_records": 120,
                "success_token_count": 205,
            },
            {
                "stat_date": "2018-08-30",
                "stat_hour": 1,
                "successful_response_records": 135,
                "success_token_count": 220,
            },
        ],
    }
    assert repository.received_codes == ["CA"]


def test_airline_timeline_returns_404_for_unknown_code() -> None:
    app = create_app(
        database=ReadyDatabase(),
        airline_timeline_repository=(
            MissingAirlineTimelineRepository()
        ),
    )
    client = TestClient(
        app,
        raise_server_exceptions=False,
    )

    response = client.get(
        "/api/v1/airlines/NOTREAL/timeline"
    )

    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "AIRLINE_NOT_FOUND",
            "message": "Airline was not found",
        }
    }
