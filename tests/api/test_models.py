import pytest
from pydantic import ValidationError
from gds_pipeline.api.models import (
    ErrorDetail,
    ErrorResponse,
    HealthResponse,
)

from gds_pipeline.api.models import HealthResponse


def test_health_response_accepts_the_contract() -> None:
    response = HealthResponse(
        status="ok",
        service="gds-analytics-api",
    )

    assert response.model_dump() == {
        "status": "ok",
        "service": "gds-analytics-api",
    }


def test_health_response_rejects_an_unknown_status() -> None:
    with pytest.raises(ValidationError):
        HealthResponse(
            status="broken",
            service="gds-analytics-api",
        )

def test_error_response_uses_a_nested_envelope() -> None:
    response = ErrorResponse(
        error=ErrorDetail(
            code="INVALID_DATE_RANGE",
            message="date_from must not be later than date_to",
        )
    )

    assert response.model_dump() == {
        "error": {
            "code": "INVALID_DATE_RANGE",
            "message": "date_from must not be later than date_to",
        }
    }
