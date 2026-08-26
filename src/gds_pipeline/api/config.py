from dataclasses import dataclass, field
import os
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True, slots=True)
class ApiSettings:
    mysql_host: str
    mysql_port: int
    mysql_database: str
    mysql_username: str
    mysql_password: str = field(repr=False)
    mysql_pool_size: int = 5
    connection_timeout: int = 10
    read_timeout: int = 30
    cors_origins: tuple[str, ...] = (
        "http://localhost:5173",
    )
    accepted_run_path: Path = Path(
        "config/accepted-run.json"
    )

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> "ApiSettings":
        values = os.environ if environment is None else environment

        origins = tuple(
            origin.strip()
            for origin in values.get(
                "GDS_API_CORS_ORIGINS",
                "http://localhost:5173",
            ).split(",")
            if origin.strip()
        )

        if not origins:
            raise ValueError(
                "GDS_API_CORS_ORIGINS must not be blank"
            )
        if "*" in origins:
            raise ValueError(
                "GDS_API_CORS_ORIGINS must not contain '*'"
            )

        accepted_run_text = values.get(
            "GDS_ACCEPTED_RUN_PATH",
            "config/accepted-run.json",
        ).strip()
        if not accepted_run_text:
            raise ValueError(
                "GDS_ACCEPTED_RUN_PATH must not be blank"
            )

        return cls(
            mysql_host=_required(
                values,
                "GDS_MYSQL_HOST",
            ),
            mysql_port=_integer(
                values,
                "GDS_MYSQL_HOST_PORT",
                default=3307,
                minimum=1,
                maximum=65535,
            ),
            mysql_database=_required(
                values,
                "MYSQL_DATABASE",
            ),
            mysql_username=_required(
                values,
                "GDS_API_MYSQL_USER",
            ),
            mysql_password=_required(
                values,
                "GDS_API_MYSQL_PASSWORD",
            ),
            mysql_pool_size=_integer(
                values,
                "GDS_API_MYSQL_POOL_SIZE",
                default=5,
                minimum=1,
            ),
            connection_timeout=_integer(
                values,
                "GDS_MYSQL_CONNECTION_TIMEOUT",
                default=10,
                minimum=1,
            ),
            read_timeout=_integer(
                values,
                "GDS_MYSQL_READ_TIMEOUT",
                default=30,
                minimum=1,
            ),
            cors_origins=origins,
            accepted_run_path=Path(accepted_run_text),
        )


def _required(
    values: Mapping[str, str],
    variable: str,
) -> str:
    value = values.get(variable, "").strip()
    if not value:
        raise ValueError(f"{variable} must not be blank")
    return value


def _integer(
    values: Mapping[str, str],
    variable: str,
    *,
    default: int,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    raw = values.get(variable, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(
            f"{variable} must be an integer"
        ) from error

    if minimum is not None and value < minimum:
        raise ValueError(
            f"{variable} must be at least {minimum}"
        )
    if maximum is not None and value > maximum:
        raise ValueError(
            f"{variable} must be at most {maximum}"
        )

    return value
