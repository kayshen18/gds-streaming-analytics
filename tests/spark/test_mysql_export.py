import hashlib
import json
from pathlib import Path

import pytest

from gds_pipeline.mysql_export import SourceIdentity, export_hdfs_snapshot
from gds_pipeline.mysql_snapshot import load_snapshot
from gds_pipeline.spark_config import DatasetPaths, SparkPipelineSettings


EXPECTED_CSV = (
    b"stat_date,stat_hour,airline_code,successful_response_records,"
    b"successful_booking_tokens\n"
    b"20160601,2,CA,1,1\n"
    b"20160601,10,CA,4,7\n"
    b"20160602,0,MU,2,3\n"
)


def _paths(root: Path) -> DatasetPaths:
    return DatasetPaths.from_settings(
        SparkPipelineSettings(
            hdfs_root=root.resolve().as_uri(),
            checkpoint_root=(root / "checkpoints").resolve().as_uri(),
        )
    )


def _write_metrics(spark, paths: DatasetPaths) -> None:
    spark.createDataFrame(
        [
            ("20160602", 0, "MU", 2, 3),
            ("20160601", 10, "CA", 4, 7),
            ("20160601", 2, "CA", 1, 1),
        ],
        [
            "event_date",
            "event_hour",
            "airline_code",
            "successful_response_records",
            "success_token_count",
        ],
    ).write.mode("overwrite").parquet(paths.hourly_airline_metrics)


def test_export_hdfs_snapshot_is_canonical_valid_and_repeatable(
    spark, tmp_path: Path
) -> None:
    paths = _paths(tmp_path / "hdfs")
    _write_metrics(spark, paths)
    destination = tmp_path / "snapshot"
    identity = SourceIdentity(
        hdfs_root="hdfs://hdfs-namenode:8020/data/gds",
        output_version="v1",
    )

    first = export_hdfs_snapshot(spark, paths, destination, identity)
    first_csv = (destination / "metrics.csv").read_bytes()
    first_manifest = (destination / "manifest.json").read_bytes()
    second = export_hdfs_snapshot(spark, paths, destination, identity)

    assert first_csv == EXPECTED_CSV
    assert (destination / "metrics.csv").read_bytes() == first_csv
    assert (destination / "manifest.json").read_bytes() == first_manifest
    assert first == second
    assert first.row_count == 3
    assert first.successful_response_records == 7
    assert first.success_token_count == 11
    assert first.metrics_sha256 == hashlib.sha256(EXPECTED_CSV).hexdigest()
    assert load_snapshot(
        destination / "metrics.csv", destination / "manifest.json"
    ).metrics_sha256 == first.metrics_sha256
    assert json.loads(first_manifest) == {
        "metrics_sha256": first.metrics_sha256,
        "output_version": "v1",
        "row_count": 3,
        "schema_version": 1,
        "source_hdfs_root": "hdfs://hdfs-namenode:8020/data/gds",
        "success_token_count": 11,
        "successful_response_records": 7,
    }


def test_export_failure_preserves_previous_complete_snapshot(
    spark, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths(tmp_path / "hdfs")
    _write_metrics(spark, paths)
    destination = tmp_path / "snapshot"
    identity = SourceIdentity("hdfs://example/data/gds", "v1")
    export_hdfs_snapshot(spark, paths, destination, identity)
    before = {
        path.name: path.read_bytes() for path in destination.iterdir()
    }

    def fail_before_publish(*args, **kwargs):
        raise RuntimeError("injected validation failure")

    monkeypatch.setattr("gds_pipeline.mysql_export.load_snapshot", fail_before_publish)

    with pytest.raises(RuntimeError, match="injected"):
        export_hdfs_snapshot(spark, paths, destination, identity)

    assert {
        path.name: path.read_bytes() for path in destination.iterdir()
    } == before
    assert not list(tmp_path.glob(".snapshot.tmp-*"))
    assert not list(tmp_path.glob(".snapshot.backup-*"))
