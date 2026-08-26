from contextlib import contextmanager
from datetime import date
from typing import Iterator

import pytest

from gds_pipeline.api.overview_repository import (
    OverviewNotAvailableError,
    OverviewRepository,
)


class FakeCursor:
    def __init__(self) -> None:
        self.statement = ""
        self.closed = False
        self.row: tuple[object, ...] | None = (
            3203,
            46,
            1310068,
            2145511,
            date(2016, 6, 1),
            date(2016, 6, 30),
            "126fa842-3721-4233-991f-8fd3b9e22929",
        )

    def execute(self, statement: str) -> None:
        self.statement = statement

    def fetchone(self) -> tuple[object, ...] | None:
        return self.row


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


def test_fetch_overview_aggregates_the_serving_snapshot() -> None:
    database = FakeDatabase()
    repository = OverviewRepository(database)

    overview = repository.fetch_overview()

    assert overview.metric_rows == 3203
    assert overview.airline_count == 46
    assert overview.successful_response_records == 1310068
    assert overview.success_token_count == 2145511
    assert overview.start_date == date(2016, 6, 1)
    assert overview.end_date == date(2016, 6, 30)
    assert overview.publication_id == (
        "126fa842-3721-4233-991f-8fd3b9e22929"
    )

    statement = (
        database.connection_value.cursor_value.statement.lower()
    )
    assert "hourly_airline_metrics" in statement
    assert "count(distinct airline_code)" in statement
    assert "sum(successful_response_records)" in statement
    assert "sum(success_token_count)" in statement
    assert "min(stat_date)" in statement
    assert "max(stat_date)" in statement

    assert database.connection_value.cursor_value.closed is True
    assert database.returned is True

def test_fetch_overview_rejects_an_empty_serving_snapshot() -> None:
    database = FakeDatabase()
    database.connection_value.cursor_value.row = (
        0,
        0,
        0,
        0,
        None,
        None,
        None,
    )
    repository = OverviewRepository(database)

    with pytest.raises(OverviewNotAvailableError):
        repository.fetch_overview()

    assert database.connection_value.cursor_value.closed is True
    assert database.returned is True
