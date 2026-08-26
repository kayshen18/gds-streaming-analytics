from fastapi import FastAPI

from gds_pipeline.api.errors import install_error_handlers
from gds_pipeline.api.models import HealthResponse


def create_app() -> FastAPI:
    app = FastAPI(
        title="GDS Streaming Analytics API",
        version="1.0.0",
    )

    install_error_handlers(app)

    @app.get(
        "/api/v1/health",
        response_model=HealthResponse,
    )
    def health() -> HealthResponse:
        return HealthResponse(
            status="ok",
            service="gds-analytics-api",
        )

    return app


app = create_app()
