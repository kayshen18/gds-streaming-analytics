"""Export the final Spark/HDFS aggregate as a canonical local snapshot."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from uuid import uuid4

from .mysql_snapshot import (
    MetricRow,
    SnapshotManifest,
    canonical_snapshot_bytes,
    load_snapshot,
)
from .spark_config import DatasetPaths


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    hdfs_root: str
    output_version: str

    def __post_init__(self) -> None:
        if not self.hdfs_root.strip():
            raise ValueError("hdfs_root must not be blank")
        if not self.output_version.strip():
            raise ValueError("output_version must not be blank")


def export_hdfs_snapshot(
    spark,
    paths: DatasetPaths,
    destination_dir: Path,
    source_identity: SourceIdentity,
) -> SnapshotManifest:
    """Collect the small final aggregate and atomically publish two local files."""

    selected = (
        spark.read.parquet(paths.hourly_airline_metrics)
        .select(
            "event_date",
            "event_hour",
            "airline_code",
            "successful_response_records",
            "success_token_count",
        )
        .orderBy("event_date", "event_hour", "airline_code")
    )
    rows = tuple(
        MetricRow(
            stat_date=str(row.event_date),
            stat_hour=row.event_hour,
            airline_code=row.airline_code,
            successful_response_records=row.successful_response_records,
            success_token_count=row.success_token_count,
        )
        for row in selected.toLocalIterator()
    )
    payload = canonical_snapshot_bytes(rows)

    destination_dir = destination_dir.resolve()
    destination_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(
            prefix=f".{destination_dir.name}.tmp-",
            dir=destination_dir.parent,
        )
    )
    backup: Path | None = None
    try:
        metrics_path = temporary / "metrics.csv"
        metrics_path.write_bytes(payload)
        _fsync_file(metrics_path)

        manifest = SnapshotManifest(
            schema_version=1,
            source_hdfs_root=source_identity.hdfs_root.rstrip("/"),
            output_version=source_identity.output_version,
            row_count=len(rows),
            successful_response_records=sum(
                row.successful_response_records for row in rows
            ),
            success_token_count=sum(row.success_token_count for row in rows),
            metrics_sha256=hashlib.sha256(payload).hexdigest(),
        )
        manifest_path = temporary / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "metrics_sha256": manifest.metrics_sha256,
                    "output_version": manifest.output_version,
                    "row_count": manifest.row_count,
                    "schema_version": manifest.schema_version,
                    "source_hdfs_root": manifest.source_hdfs_root,
                    "success_token_count": manifest.success_token_count,
                    "successful_response_records": (
                        manifest.successful_response_records
                    ),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
            newline="\n",
        )
        _fsync_file(manifest_path)
        validated = load_snapshot(metrics_path, manifest_path)

        if destination_dir.exists():
            backup = destination_dir.with_name(
                f".{destination_dir.name}.backup-{uuid4().hex}"
            )
            os.replace(destination_dir, backup)
        os.replace(temporary, destination_dir)
        _fsync_directory(destination_dir.parent)
        if backup is not None:
            shutil.rmtree(backup)
        return validated.manifest
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        if backup is not None and backup.exists() and not destination_dir.exists():
            os.replace(backup, destination_dir)
        raise


def _fsync_file(path: Path) -> None:
    with path.open("rb") as source:
        os.fsync(source.fileno())


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
