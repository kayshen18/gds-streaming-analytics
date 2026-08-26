from datetime import date

from fastapi.testclient import TestClient

from gds_pipeline.api.main import create_app
from gds_pipeline.api.models import OverviewResponse
from gds_pipeline.api.overview_repository import (
    OverviewNotAvailableError,
)

class ReadyDatabase:
    def is_ready(self) -> bool:
        return True


class FakeOverviewRepository:
    def __init__(self) -> None:
        self.called = False

    def fetch_overview(self) -> OverviewResponse:
        self.called = True
        return OverviewResponse(
            metric_rows=3203,
            airline_count=46,
            successful_response_records=1310068,
            success_token_count=2145511,
            start_date=date(2016, 6, 1),
            end_date=date(2016, 6, 30),
            publication_id=(
                "126fa842-3721-4233-991f-8fd3b9e22929"
            ),
        )

class EmptyOverviewRepository:
    def fetch_overview(self) -> OverviewResponse:
        raise OverviewNotAvailableError(
            "No overview snapshot is available"
        )

def test_overview_returns_dashboard_summary() -> None:
    repository = FakeOverviewRepository()
    app = create_app(
        database=ReadyDatabase(),
        overview_repository=repository,
    )
    client = TestClient(app)

    response = client.get("/api/v1/overview")

    assert response.status_code == 200
    assert response.json() == {
        "metric_rows": 3203,
        "airline_count": 46,
        "successful_response_records": 1310068,
        "success_token_count": 2145511,
        "start_date": "2016-06-01",
        "end_date": "2016-06-30",
        "publication_id": (
            "126fa842-3721-4233-991f-8fd3b9e22929"
        ),
    }
    assert repository.called is True

def test_overview_returns_404_when_snapshot_is_empty() -> None:
    app = create_app(
        database=ReadyDatabase(),
        overview_repository=EmptyOverviewRepository(),
    )
    client = TestClient(
        app,
        raise_server_exceptions=False,
    )

    response = client.get("/api/v1/overview")

    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "OVERVIEW_NOT_AVAILABLE",
            "message": "Overview data is not available",
        }
    }
