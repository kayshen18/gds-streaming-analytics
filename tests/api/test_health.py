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

def test_health_documents_typed_response() -> None:
    client = TestClient(create_app())

    openapi = client.get("/openapi.json").json()
    response_schema = openapi["paths"]["/api/v1/health"]["get"][
        "responses"
    ]["200"]["content"]["application/json"]["schema"]

    assert response_schema == {
        "$ref": "#/components/schemas/HealthResponse"
    }
