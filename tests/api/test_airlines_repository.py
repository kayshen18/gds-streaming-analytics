from contextlib import contextmanager
from typing import Iterator

from gds_pipeline.api.airlines_repository import AirlinesRepository


class FakeCursor:
    def __init__(self) -> None:
        self.executions: list[
            tuple[str, tuple[object, ...] | None]
        ] = []
        self.closed = False

    def execute(
        self,
        statement: str,
        parameters: tuple[object, ...] | None = None,
    ) -> None:
        self.executions.append((statement, parameters))

    def fetchone(self) -> tuple[int]:
        return (198,)

    def fetchall(self) -> list[tuple[object, ...]]:
        return [
            ("CA", 500, 820),
            ("MU", 420, 700),
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


def test_fetch_airlines_returns_ranked_limited_results() -> None:
    database = FakeDatabase()
    repository = AirlinesRepository(database)

    response = repository.fetch_airlines(limit=20)

    assert response.total_airlines == 198
    assert len(response.items) == 2
    assert response.items[0].airline_code == "CA"
    assert response.items[0].successful_response_records == 500
    assert response.items[0].success_token_count == 820
    assert response.items[1].airline_code == "MU"

    executions = database.connection_value.cursor_value.executions
    assert len(executions) == 2

    count_sql, count_parameters = executions[0]
    ranking_sql, ranking_parameters = executions[1]

    assert "count(distinct airline_code)" in count_sql.lower()
    assert count_parameters is None

    normalized_ranking_sql = ranking_sql.lower()
    assert "group by airline_code" in normalized_ranking_sql
    assert "sum(successful_response_records)" in (
        normalized_ranking_sql
    )
    assert "order by successful_response_records desc" in (
        normalized_ranking_sql
    )
    assert "limit %s" in normalized_ranking_sql
    assert ranking_parameters == (20,)

    assert database.connection_value.cursor_value.closed is True
    assert database.returned is True
