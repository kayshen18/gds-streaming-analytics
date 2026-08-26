import pytest
from pydantic import ValidationError
from gds_pipeline.api.models import (
    ErrorDetail,
    ErrorResponse,
    HealthResponse,
    OverviewResponse,
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

def test_overview_response_accepts_dashboard_summary() -> None:
    response = OverviewResponse(
        metric_rows=3203,
        airline_count=46,
        successful_response_records=1310068,
        success_token_count=2145511,
        start_date="2016-06-01",
        end_date="2016-06-30",
        publication_id="126fa842-3721-4233-991f-8fd3b9e22929",
    )

    assert response.metric_rows == 3203
    assert response.airline_count == 46
    assert response.successful_response_records == 1310068
    assert response.success_token_count == 2145511
    assert response.start_date.isoformat() == "2016-06-01"
    assert response.end_date.isoformat() == "2016-06-30"


def test_overview_response_rejects_negative_counts() -> None:
    with pytest.raises(ValidationError):
        OverviewResponse(
            metric_rows=-1,
            airline_count=46,
            successful_response_records=1310068,
            success_token_count=2145511,
            start_date="2016-06-01",
            end_date="2016-06-30",
            publication_id="126fa842-3721-4233-991f-8fd3b9e22929",
        )
