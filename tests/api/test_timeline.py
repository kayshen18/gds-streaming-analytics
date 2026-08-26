from datetime import date

from fastapi.testclient import TestClient

from gds_pipeline.api.main import create_app
from gds_pipeline.api.models import (
    TimelinePoint,
    TimelineResponse,
)


class ReadyDatabase:
    def is_ready(self) -> bool:
        return True


class FakeTimelineRepository:
    def __init__(self) -> None:
        self.called = False

    def fetch_timeline(self) -> TimelineResponse:
        self.called = True
        return TimelineResponse(
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
            ]
        )


def test_timeline_returns_hourly_points() -> None:
    repository = FakeTimelineRepository()
    app = create_app(
        database=ReadyDatabase(),
        timeline_repository=repository,
    )
    client = TestClient(app)

    response = client.get("/api/v1/timeline")

    assert response.status_code == 200
    assert response.json() == {
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
        ]
    }
    assert repository.called is True
