from contextlib import contextmanager
from datetime import date
from typing import Iterator

from gds_pipeline.api.timeline_repository import TimelineRepository


class FakeCursor:
    def __init__(self) -> None:
        self.statement = ""
        self.closed = False

    def execute(self, statement: str) -> None:
        self.statement = statement

    def fetchall(self) -> list[tuple[object, ...]]:
        return [
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


def test_fetch_timeline_groups_and_orders_hourly_metrics() -> None:
    database = FakeDatabase()
    repository = TimelineRepository(database)

    timeline = repository.fetch_timeline()

    assert len(timeline.items) == 2
    assert timeline.items[0].stat_date == date(2018, 8, 30)
    assert timeline.items[0].stat_hour == 0
    assert timeline.items[0].successful_response_records == 120
    assert timeline.items[0].success_token_count == 205
    assert timeline.items[1].stat_hour == 1

    statement = (
        database.connection_value.cursor_value.statement.lower()
    )
    assert "hourly_airline_metrics" in statement
    assert "sum(successful_response_records)" in statement
    assert "sum(success_token_count)" in statement
    assert "group by stat_date, stat_hour" in statement
    assert "order by stat_date, stat_hour" in statement

    assert database.connection_value.cursor_value.closed is True
    assert database.returned is True
