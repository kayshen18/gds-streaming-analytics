from fastapi.testclient import TestClient

from gds_pipeline.api.errors import ApiError
from gds_pipeline.api.main import create_app


def test_api_error_uses_the_common_envelope() -> None:
    app = create_app()

    @app.get("/api/v1/_test/business-error")
    def business_error() -> None:
        raise ApiError(
            status_code=400,
            code="INVALID_DATE_RANGE",
            message="date_from must not be later than date_to",
        )

    client = TestClient(app)
    response = client.get("/api/v1/_test/business-error")

    assert response.status_code == 400
    assert response.json() == {
        "error": {
            "code": "INVALID_DATE_RANGE",
            "message": "date_from must not be later than date_to",
        }
    }


def test_internal_error_does_not_leak_sensitive_details() -> None:
    app = create_app()

    @app.get("/api/v1/_test/internal-error")
    def internal_error() -> None:
        raise RuntimeError(
            "SELECT password FROM secret_credentials"
        )

    client = TestClient(
        app,
        raise_server_exceptions=False,
    )
    response = client.get("/api/v1/_test/internal-error")

    assert response.status_code == 500
    assert response.json() == {
        "error": {
            "code": "INTERNAL_SERVER_ERROR",
            "message": "An unexpected error occurred",
        }
    }
    assert "password" not in response.text
    assert "secret_credentials" not in response.text

def test_missing_route_uses_the_common_envelope() -> None:
    client = TestClient(create_app())

    response = client.get("/api/v1/does-not-exist")

    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "NOT_FOUND",
            "message": "Resource not found",
        }
    }


def test_invalid_request_uses_the_common_envelope() -> None:
    app = create_app()

    @app.get("/api/v1/_test/number")
    def number(limit: int) -> dict[str, int]:
        return {"limit": limit}

    client = TestClient(app)
    response = client.get("/api/v1/_test/number?limit=abc")

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "INVALID_REQUEST",
            "message": "Request validation failed",
        }
    }
