import pytest
from contextlib import contextmanager
from datetime import date
from typing import Iterator

from gds_pipeline.api.airline_timeline_repository import (
    AirlineNotFoundError,
    AirlineTimelineRepository,
)


class FakeCursor:
    def __init__(self) -> None:
        self.statement = ""
        self.parameters: tuple[object, ...] | None = None
        self.closed = False
        self.rows: list[tuple[object, ...]] = [
            (
                date(2018, 8, 30),
                0,
                120,
                205,
            ),
            (
                date(2018, 8, 30),
                1,
                135,
                220,
            ),
        ]

    def execute(
        self,
        statement: str,
        parameters: tuple[object, ...] | None = None,
    ) -> None:
        self.statement = statement
        self.parameters = parameters

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_value = FakeCursor()

    def cursor(self) -> FakeCursor:
        return self.cursor_value


class FakeDatabase:
    def __init__(self) -> None:
        self.connection_value = FakeConnection()
        self.returned = False

    @contextmanager
    def connection(self) -> Iterator[FakeConnection]:
        try:
            yield self.connection_value
        finally:
            self.returned = True


def test_fetch_airline_timeline_filters_and_orders_points() -> None:
    database = FakeDatabase()
    repository = AirlineTimelineRepository(database)

    response = repository.fetch_airline_timeline(
        airline_code="CA"
    )

    assert response.airline_code == "CA"
    assert len(response.items) == 2
    assert response.items[0].stat_hour == 0
    assert response.items[0].successful_response_records == 120
    assert response.items[1].stat_hour == 1

    cursor = database.connection_value.cursor_value
    statement = cursor.statement.lower()

    assert "hourly_airline_metrics" in statement
    assert "where airline_code = %s" in statement
    assert "order by stat_date, stat_hour" in statement
    assert cursor.parameters == ("CA",)
    assert cursor.closed is True
    assert database.returned is True

def test_fetch_airline_timeline_rejects_unknown_airline() -> None:
    database = FakeDatabase()
    database.connection_value.cursor_value.rows = []
    repository = AirlineTimelineRepository(database)

    with pytest.raises(AirlineNotFoundError):
        repository.fetch_airline_timeline(
            airline_code="NOTREAL"
        )

    assert database.connection_value.cursor_value.closed is True
    assert database.returned is True
