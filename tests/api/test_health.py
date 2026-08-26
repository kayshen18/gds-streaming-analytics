from fastapi.testclient import TestClient

from gds_pipeline.api.main import create_app, create_runtime_app


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

class ReadyDatabase:
    def __init__(self) -> None:
        self.checked = False

    def is_ready(self) -> bool:
        self.checked = True
        return True


class UnavailableDatabase:
    def is_ready(self) -> bool:
        raise RuntimeError(
            "database password and SQL must stay private"
        )


def test_health_checks_database_readiness() -> None:
    database = ReadyDatabase()
    client = TestClient(create_app(database=database))

    response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert database.checked is True


def test_health_returns_503_without_leaking_database_error() -> None:
    client = TestClient(
        create_app(database=UnavailableDatabase()),
    )

    response = client.get("/api/v1/health")

    assert response.status_code == 503
    assert response.json() == {
        "error": {
            "code": "DATABASE_UNAVAILABLE",
            "message": "Database is unavailable",
        }
    }
    assert "password" not in response.text
    assert "SQL" not in response.text

def test_runtime_app_uses_real_database_factory() -> None:
    settings = object()
    database = ReadyDatabase()
    received: list[object] = []

    def load_settings() -> object:
        return settings

    def create_database(received_settings: object) -> ReadyDatabase:
        received.append(received_settings)
        return database

    app = create_runtime_app(
        settings_loader=load_settings,
        database_factory=create_database,
    )
    client = TestClient(app)

    response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert database.checked is True
    assert received == [settings]
