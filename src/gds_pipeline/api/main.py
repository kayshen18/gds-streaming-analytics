from fastapi import FastAPI


def create_app() -> FastAPI:
    app = FastAPI(
        title="GDS Streaming Analytics API",
        version="1.0.0",
    )

    @app.get("/api/v1/health")
    def health() -> dict[str, str]:
        return {
            "status": "ok",
            "service": "gds-analytics-api",
        }

    return app


app = create_app()
