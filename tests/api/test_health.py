from fastapi.testclient import TestClient

from gds_pipeline.api.main import create_app


def test_health_returns_service_status() -> None:
    client = TestClient(create_app())

    response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "gds-analytics-api",
    }
