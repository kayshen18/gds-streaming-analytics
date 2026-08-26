from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Protocol
from mysql.connector.pooling import MySQLConnectionPool

from gds_pipeline.api.config import ApiSettings


class Cursor(Protocol):
    def execute(self, statement: str) -> None:
        ...

    def fetchone(self) -> object:
        ...

    def close(self) -> None:
        ...


class Connection(Protocol):
    def cursor(self) -> Cursor:
        ...

    def close(self) -> None:
        ...


class Pool(Protocol):
    def get_connection(self) -> Connection:
        ...

class ReadinessProbe(Protocol):
    def is_ready(self) -> bool:
        ...

class DatabasePool:
    def __init__(self, pool: Pool) -> None:
        self._pool = pool

    @contextmanager
    def connection(self) -> Iterator[Connection]:
        connection = self._pool.get_connection()
        try:
            yield connection
        finally:
            connection.close()

    def is_ready(self) -> bool:
        with self.connection() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute("SELECT 1")
                return cursor.fetchone() == (1,)
            finally:
                cursor.close()

def create_database_pool(
    settings: ApiSettings,
    *,
    pool_factory: Callable[..., Pool] = MySQLConnectionPool,
) -> DatabasePool:
    pool = pool_factory(
        pool_name="gds_api",
        pool_size=settings.mysql_pool_size,
        pool_reset_session=True,
        host=settings.mysql_host,
        port=settings.mysql_port,
        database=settings.mysql_database,
        user=settings.mysql_username,
        password=settings.mysql_password,
        connection_timeout=settings.connection_timeout,
        read_timeout=settings.read_timeout,
        autocommit=True,
        charset="utf8mb4",
        use_unicode=True,
    )
    return DatabasePool(pool)
