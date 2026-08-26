from datetime import datetime

from fastapi.testclient import TestClient

from gds_pipeline.api.main import create_app
from gds_pipeline.api.models import PublicationResponse
from gds_pipeline.api.publication_repository import (
    PublicationNotAvailableError,
)


class ReadyDatabase:
    def is_ready(self) -> bool:
        return True


class FakePublicationRepository:
    def __init__(self) -> None:
        self.called = False

    def fetch_publication(self) -> PublicationResponse:
        self.called = True

        return PublicationResponse(
            publication_id=(
                "126fa842-3721-4233-991f-8fd3b9e22929"
            ),
            source_hdfs_root=(
                "hdfs://hdfs-namenode:8020/data/gds/metrics"
            ),
            output_version="v1",
            source_row_count=3203,
            successful_response_records=1310068,
            success_token_count=2145511,
            metrics_sha256=(
                "9b0f4a3afc33e73461414ff2d60a2653"
                "e32a5fdbcfe8a810b8b2b42525fcc0be"
            ),
            status="published",
            completed_at=datetime(
                2026,
                8,
                13,
                14,
                15,
                19,
                385388,
            ),
        )


class MissingPublicationRepository:
    def fetch_publication(self) -> PublicationResponse:
        raise PublicationNotAvailableError(
            "No published snapshot is available"
        )


def test_publication_returns_latest_snapshot_metadata() -> None:
    repository = FakePublicationRepository()
    app = create_app(
        database=ReadyDatabase(),
        publication_repository=repository,
    )
    client = TestClient(app)

    response = client.get("/api/v1/publication")

    assert response.status_code == 200
    assert response.json() == {
        "publication_id": (
            "126fa842-3721-4233-991f-8fd3b9e22929"
        ),
        "source_hdfs_root": (
            "hdfs://hdfs-namenode:8020/data/gds/metrics"
        ),
        "output_version": "v1",
        "source_row_count": 3203,
        "successful_response_records": 1310068,
        "success_token_count": 2145511,
        "metrics_sha256": (
            "9b0f4a3afc33e73461414ff2d60a2653"
            "e32a5fdbcfe8a810b8b2b42525fcc0be"
        ),
        "status": "published",
        "completed_at": "2026-08-13T14:15:19.385388",
    }
    assert repository.called is True


def test_publication_returns_404_when_snapshot_is_missing() -> None:
    app = create_app(
        database=ReadyDatabase(),
        publication_repository=(
            MissingPublicationRepository()
        ),
    )
    client = TestClient(
        app,
        raise_server_exceptions=False,
    )

    response = client.get("/api/v1/publication")

    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "PUBLICATION_NOT_AVAILABLE",
            "message": "Publication data is not available",
        }
    }
