from fastapi.testclient import TestClient

from gds_pipeline.api.main import create_app
from gds_pipeline.api.models import (
    AirlineSummary,
    AirlinesResponse,
)


class ReadyDatabase:
    def is_ready(self) -> bool:
        return True


class FakeAirlinesRepository:
    def __init__(self) -> None:
        self.received_limits: list[int] = []

    def fetch_airlines(self, *, limit: int) -> AirlinesResponse:
        self.received_limits.append(limit)
        return AirlinesResponse(
            total_airlines=198,
            items=[
                AirlineSummary(
                    airline_code="CA",
                    successful_response_records=500,
                    success_token_count=820,
                ),
                AirlineSummary(
                    airline_code="MU",
                    successful_response_records=420,
                    success_token_count=700,
                ),
            ],
        )


def test_airlines_returns_ranked_results_with_limit() -> None:
    repository = FakeAirlinesRepository()
    app = create_app(
        database=ReadyDatabase(),
        airlines_repository=repository,
    )
    client = TestClient(app)

    response = client.get("/api/v1/airlines?limit=5")

    assert response.status_code == 200
    assert response.json() == {
        "total_airlines": 198,
        "items": [
            {
                "airline_code": "CA",
                "successful_response_records": 500,
                "success_token_count": 820,
            },
            {
                "airline_code": "MU",
                "successful_response_records": 420,
                "success_token_count": 700,
            },
        ],
    }
    assert repository.received_limits == [5]


def test_airlines_rejects_limit_outside_allowed_range() -> None:
    repository = FakeAirlinesRepository()
    app = create_app(
        database=ReadyDatabase(),
        airlines_repository=repository,
    )
    client = TestClient(app)

    response = client.get("/api/v1/airlines?limit=0")

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "INVALID_REQUEST",
            "message": "Request validation failed",
        }
    }
    assert repository.received_limits == []
