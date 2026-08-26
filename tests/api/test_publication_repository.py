import pytest
from contextlib import contextmanager
from datetime import datetime
from typing import Iterator

from gds_pipeline.api.publication_repository import (
    PublicationNotAvailableError,
    PublicationRepository,
)


class FakeCursor:
    def __init__(self) -> None:
        self.statement = ""
        self.parameters: tuple[object, ...] | None = None
        self.closed = False
        self.row: tuple[object, ...] | None = (
            "126fa842-3721-4233-991f-8fd3b9e22929",
            "hdfs://hdfs-namenode:8020/data/gds/metrics",
            "v1",
            3203,
            1310068,
            2145511,
            (
                "9b0f4a3afc33e73461414ff2d60a2653"
                "e32a5fdbcfe8a810b8b2b42525fcc0be"
            ),
            "published",
            datetime(2026, 8, 13, 14, 15, 19, 385388),
        )

    def execute(
        self,
        statement: str,
        parameters: tuple[object, ...] | None = None,
    ) -> None:
        self.statement = statement
        self.parameters = parameters

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


def test_fetch_publication_returns_latest_published_snapshot() -> None:
    database = FakeDatabase()
    repository = PublicationRepository(database)

    publication = repository.fetch_publication()

    assert publication.publication_id == (
        "126fa842-3721-4233-991f-8fd3b9e22929"
    )
    assert publication.source_row_count == 3203
    assert publication.successful_response_records == 1310068
    assert publication.success_token_count == 2145511
    assert publication.status == "published"
    assert publication.completed_at.year == 2026

    cursor = database.connection_value.cursor_value
    statement = cursor.statement.lower()

    assert "metric_publications" in statement
    assert "where status = %s" in statement
    assert "order by completed_at desc" in statement
    assert "limit 1" in statement
    assert cursor.parameters == ("published",)
    assert cursor.closed is True
    assert database.returned is True

def test_fetch_publication_rejects_missing_snapshot() -> None:
    database = FakeDatabase()
    database.connection_value.cursor_value.row = None
    repository = PublicationRepository(database)

    with pytest.raises(PublicationNotAvailableError):
        repository.fetch_publication()

    assert database.connection_value.cursor_value.closed is True
    assert database.returned is True
