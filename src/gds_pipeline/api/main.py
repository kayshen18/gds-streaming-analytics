from collections.abc import Callable

from fastapi import FastAPI

from gds_pipeline.api.config import ApiSettings
from gds_pipeline.api.database import (
    ReadinessProbe,
    create_database_pool,
)
from gds_pipeline.api.errors import (
    ApiError,
    install_error_handlers,
)
from gds_pipeline.api.models import (
    HealthResponse,
    OverviewResponse,
    TimelineResponse,
)
from gds_pipeline.api.overview_repository import (
    OverviewNotAvailableError,
    OverviewReader,
    OverviewRepository,
)
from gds_pipeline.api.timeline_repository import (
    TimelineReader,
    TimelineRepository,
)


class AlwaysReadyDatabase:
    def is_ready(self) -> bool:
        return True


def create_app(
    database: ReadinessProbe | None = None,
    overview_repository: OverviewReader | None = None,
    timeline_repository: TimelineReader | None = None,
) -> FastAPI:
    app = FastAPI(
        title="GDS Streaming Analytics API",
        version="1.0.0",
    )

    install_error_handlers(app)

    if database is None:
        database = AlwaysReadyDatabase()

    if overview_repository is None:
        overview_repository = OverviewRepository(database)

    if timeline_repository is None:
        timeline_repository = TimelineRepository(database)

    @app.get(
        "/api/v1/health",
        response_model=HealthResponse,
    )
    def health() -> HealthResponse:
        try:
            ready = database.is_ready()
        except Exception as error:
            raise ApiError(
                status_code=503,
                code="DATABASE_UNAVAILABLE",
                message="Database is unavailable",
            ) from error

        if not ready:
            raise ApiError(
                status_code=503,
                code="DATABASE_UNAVAILABLE",
                message="Database is unavailable",
            )

        return HealthResponse(
            status="ok",
            service="gds-analytics-api",
        )
    @app.get(
        "/api/v1/overview",
        response_model=OverviewResponse,
    )
    def overview() -> OverviewResponse:
        try:
            return overview_repository.fetch_overview()
        except OverviewNotAvailableError as error:
            raise ApiError(
                status_code=404,
                code="OVERVIEW_NOT_AVAILABLE",
                message="Overview data is not available",
            ) from error

    @app.get(
        "/api/v1/timeline",
        response_model=TimelineResponse,
    )
    def timeline() -> TimelineResponse:
        return timeline_repository.fetch_timeline()

    return app


def create_runtime_app(
    settings_loader: Callable[[], ApiSettings] = (
        ApiSettings.from_environment
    ),
    database_factory: Callable[
        [ApiSettings],
        ReadinessProbe,
    ] = create_database_pool,
) -> FastAPI:
    settings = settings_loader()
    database = database_factory(settings)
    return create_app(database=database)

app = create_app()
