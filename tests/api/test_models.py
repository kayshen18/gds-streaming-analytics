import pytest
from pydantic import ValidationError
from gds_pipeline.api.models import (
    AirlineSummary,
    AirlinesResponse,
    AirlineTimelineResponse,
    ErrorDetail,
    ErrorResponse,
    HealthResponse,
    HeatmapCell,
    HourlyHeatmapResponse,
    OverviewResponse,
    TimelinePoint,
    TimelineResponse,
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

def test_timeline_response_accepts_hourly_points() -> None:
    response = TimelineResponse(
        items=[
            TimelinePoint(
                stat_date="2018-08-30",
                stat_hour=0,
                successful_response_records=120,
                success_token_count=205,
            ),
            TimelinePoint(
                stat_date="2018-08-30",
                stat_hour=1,
                successful_response_records=135,
                success_token_count=220,
            ),
        ]
    )

    assert len(response.items) == 2
    assert response.items[0].stat_hour == 0
    assert response.items[1].stat_hour == 1
    assert response.items[0].stat_date.isoformat() == "2018-08-30"


@pytest.mark.parametrize("invalid_hour", [-1, 24])
def test_timeline_point_rejects_invalid_hours(
    invalid_hour: int,
) -> None:
    with pytest.raises(ValidationError):
        TimelinePoint(
            stat_date="2018-08-30",
            stat_hour=invalid_hour,
            successful_response_records=120,
            success_token_count=205,
        )

def test_airlines_response_accepts_ranked_summaries() -> None:
    response = AirlinesResponse(
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

    assert response.total_airlines == 198
    assert len(response.items) == 2
    assert response.items[0].airline_code == "CA"
    assert response.items[1].successful_response_records == 420


def test_airline_summary_rejects_blank_code() -> None:
    with pytest.raises(ValidationError):
        AirlineSummary(
            airline_code="",
            successful_response_records=500,
            success_token_count=820,
        )

def test_airline_timeline_response_identifies_airline() -> None:
    response = AirlineTimelineResponse(
        airline_code="CA",
        items=[
            TimelinePoint(
                stat_date="2018-08-30",
                stat_hour=0,
                successful_response_records=120,
                success_token_count=205,
            ),
            TimelinePoint(
                stat_date="2018-08-30",
                stat_hour=1,
                successful_response_records=135,
                success_token_count=220,
            ),
        ],
    )

    assert response.airline_code == "CA"
    assert len(response.items) == 2
    assert response.items[0].stat_hour == 0


def test_hourly_heatmap_response_accepts_axes_and_cells() -> None:
    response = HourlyHeatmapResponse(
        airlines=["CZ", "MU"],
        hours=[0, 1],
        cells=[
            HeatmapCell(
                airline_code="CZ",
                stat_hour=0,
                successful_response_records=500,
                success_token_count=820,
            ),
            HeatmapCell(
                airline_code="CZ",
                stat_hour=1,
                successful_response_records=420,
                success_token_count=700,
            ),
            HeatmapCell(
                airline_code="MU",
                stat_hour=0,
                successful_response_records=390,
                success_token_count=650,
            ),
            HeatmapCell(
                airline_code="MU",
                stat_hour=1,
                successful_response_records=360,
                success_token_count=610,
            ),
        ],
    )

    assert response.airlines == ["CZ", "MU"]
    assert response.hours == [0, 1]
    assert len(response.cells) == 4
    assert response.cells[0].airline_code == "CZ"
    assert response.cells[0].stat_hour == 0
