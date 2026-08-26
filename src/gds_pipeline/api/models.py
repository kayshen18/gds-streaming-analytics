from datetime import date
from typing import Annotated, Literal

from pydantic import BaseModel, Field

NonNegativeInt = Annotated[int, Field(ge=0)]
HourOfDay = Annotated[int, Field(ge=0, le=23)]
AirlineCode = Annotated[str, Field(min_length=1, max_length=8)]


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail


class HealthResponse(BaseModel):
    status: Literal["ok"]
    service: Literal["gds-analytics-api"]


class OverviewResponse(BaseModel):
    metric_rows: NonNegativeInt
    airline_count: NonNegativeInt
    successful_response_records: NonNegativeInt
    success_token_count: NonNegativeInt
    start_date: date
    end_date: date
    publication_id: str

class TimelinePoint(BaseModel):
    stat_date: date
    stat_hour: HourOfDay
    successful_response_records: NonNegativeInt
    success_token_count: NonNegativeInt


class TimelineResponse(BaseModel):
    items: list[TimelinePoint]

class AirlineSummary(BaseModel):
    airline_code: AirlineCode
    successful_response_records: NonNegativeInt
    success_token_count: NonNegativeInt


class AirlinesResponse(BaseModel):
    total_airlines: NonNegativeInt
    items: list[AirlineSummary]

class AirlineTimelineResponse(BaseModel):
    airline_code: AirlineCode
    items: list[TimelinePoint]

class HeatmapCell(BaseModel):
    airline_code: AirlineCode
    stat_hour: HourOfDay
    successful_response_records: NonNegativeInt
    success_token_count: NonNegativeInt


class HourlyHeatmapResponse(BaseModel):
    airlines: list[AirlineCode]
    hours: list[HourOfDay]
    cells: list[HeatmapCell]
