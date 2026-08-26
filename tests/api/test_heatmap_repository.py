from contextlib import contextmanager
from typing import Iterator

from gds_pipeline.api.heatmap_repository import HeatmapRepository


class FakeCursor:
    def __init__(self) -> None:
        self.statement = ""
        self.parameters: tuple[object, ...] | None = None
        self.closed = False

    def execute(
        self,
        statement: str,
        parameters: tuple[object, ...] | None = None,
    ) -> None:
        self.statement = statement
        self.parameters = parameters

    def fetchall(self) -> list[tuple[object, ...]]:
        return [
            ("CZ", 0, 500, 820),
            ("CZ", 1, 420, 700),
            ("MU", 0, 390, 650),
            ("MU", 1, 360, 610),
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


def test_fetch_heatmap_selects_ranked_airlines_and_hours() -> None:
    database = FakeDatabase()
    repository = HeatmapRepository(database)

    response = repository.fetch_heatmap(limit=10)

    assert response.airlines == ["CZ", "MU"]
    assert response.hours == [0, 1]
    assert len(response.cells) == 4

    assert response.cells[0].airline_code == "CZ"
    assert response.cells[0].stat_hour == 0
    assert (
        response.cells[0].successful_response_records
        == 500
    )
    assert response.cells[-1].airline_code == "MU"
    assert response.cells[-1].stat_hour == 1

    cursor = database.connection_value.cursor_value
    statement = cursor.statement.lower()

    assert "row_number() over" in statement
    assert "sum(successful_response_records)" in statement
    assert "where airline_rank <= %s" in statement
    assert "group by" in statement
    assert "order by airline_rank, stat_hour" in statement
    assert cursor.parameters == (10,)
    assert cursor.closed is True
    assert database.returned is True
