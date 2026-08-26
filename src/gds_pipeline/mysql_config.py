"""Validated MySQL connection and publication settings."""

from __future__ import annotations

from dataclasses import dataclass, field
import os
from typing import Mapping


@dataclass(frozen=True, slots=True)
class MySQLSettings:
    host: str
    port: int
    database: str
    username: str
    password: str = field(repr=False)
    connection_timeout: int = 10
    read_timeout: int = 30
    write_timeout: int = 30
    batch_size: int = 500
    lock_timeout: int = 10

    @classmethod
    def from_environment(
        cls, environment: Mapping[str, str] | None = None
    ) -> "MySQLSettings":
        values = os.environ if environment is None else environment
        required = {
            "host": "GDS_MYSQL_HOST",
            "database": "MYSQL_DATABASE",
            "username": "MYSQL_USER",
            "password": "MYSQL_PASSWORD",
        }
        text_values: dict[str, str] = {}
        for field_name, variable in required.items():
            value = values.get(variable, "").strip()
            if not value:
                raise ValueError(f"{variable} must not be blank")
            text_values[field_name] = value

        port = _integer(values, "GDS_MYSQL_HOST_PORT", 3307, minimum=1)
        if port > 65535:
            raise ValueError("GDS_MYSQL_HOST_PORT must be at most 65535")
        return cls(
            **text_values,
            port=port,
            connection_timeout=_integer(
                values, "GDS_MYSQL_CONNECTION_TIMEOUT", 10, minimum=1
            ),
            read_timeout=_integer(
                values, "GDS_MYSQL_READ_TIMEOUT", 30, minimum=1
            ),
            write_timeout=_integer(
                values, "GDS_MYSQL_WRITE_TIMEOUT", 30, minimum=1
            ),
            batch_size=_integer(
                values, "GDS_MYSQL_BATCH_SIZE", 500, minimum=1
            ),
            lock_timeout=_integer(
                values, "GDS_MYSQL_LOCK_TIMEOUT", 10, minimum=0
            ),
        )

    def connector_arguments(self) -> dict[str, object]:
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.username,
            "password": self.password,
            "connection_timeout": self.connection_timeout,
            "read_timeout": self.read_timeout,
            "write_timeout": self.write_timeout,
            "autocommit": False,
            "charset": "utf8mb4",
            "use_unicode": True,
        }


def _integer(
    values: Mapping[str, str], variable: str, default: int, *, minimum: int
) -> int:
    raw = values.get(variable, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{variable} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{variable} must be at least {minimum}")
    return value
