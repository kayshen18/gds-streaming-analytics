"""Canonical snapshot model and validation for MySQL publication."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
import hashlib
import io
import json
from pathlib import Path
import re
from typing import Iterable


SNAPSHOT_HEADER = (
    "stat_date",
    "stat_hour",
    "airline_code",
    "successful_response_records",
    "successful_booking_tokens",
)
_AIRLINE_CODE = re.compile(r"^[A-Z0-9]{1,8}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class SnapshotValidationError(ValueError):
    """Raised when snapshot bytes, rows, or manifest violate the contract."""


@dataclass(frozen=True, slots=True)
class MetricRow:
    stat_date: str
    stat_hour: int
    airline_code: str
    successful_response_records: int
    success_token_count: int

    @property
    def key(self) -> tuple[str, int, str]:
        return (self.stat_date, self.stat_hour, self.airline_code)


@dataclass(frozen=True, slots=True)
class SnapshotManifest:
    schema_version: int
    source_hdfs_root: str
    output_version: str
    row_count: int
    successful_response_records: int
    success_token_count: int
    metrics_sha256: str


@dataclass(frozen=True, slots=True)
class SnapshotExpectations:
    row_count: int | None = None
    successful_response_records: int | None = None
    success_token_count: int | None = None
    metrics_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class ValidatedSnapshot:
    rows: tuple[MetricRow, ...]
    manifest: SnapshotManifest
    row_count: int
    successful_response_records: int
    success_token_count: int
    metrics_sha256: str


def canonical_snapshot_bytes(rows: Iterable[MetricRow]) -> bytes:
    """Serialize rows using the established deterministic CSV contract."""

    ordered = sorted(rows, key=lambda row: row.key)
    target = io.StringIO(newline="")
    writer = csv.writer(target, lineterminator="\n")
    writer.writerow(SNAPSHOT_HEADER)
    for row in ordered:
        _validate_row(row)
        writer.writerow(
            (
                row.stat_date,
                row.stat_hour,
                row.airline_code,
                row.successful_response_records,
                row.success_token_count,
            )
        )
    return target.getvalue().encode("utf-8")


def load_snapshot(
    csv_path: Path,
    manifest_path: Path,
    expectations: SnapshotExpectations | None = None,
) -> ValidatedSnapshot:
    """Load and independently validate a canonical CSV and its manifest."""

    try:
        payload = csv_path.read_bytes()
        text = payload.decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise SnapshotValidationError(f"cannot read UTF-8 snapshot: {exc}") from exc

    if b"\r" in payload:
        raise SnapshotValidationError("snapshot must use LF line endings")

    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        header = tuple(next(reader))
    except StopIteration as exc:
        raise SnapshotValidationError("snapshot is missing its header") from exc
    if header != SNAPSHOT_HEADER:
        raise SnapshotValidationError(f"unexpected snapshot header: {header!r}")

    rows = tuple(_parse_row(values, line_number) for line_number, values in enumerate(reader, 2))
    if not rows:
        raise SnapshotValidationError("snapshot must contain at least one row")

    keys = [row.key for row in rows]
    if len(set(keys)) != len(keys):
        raise SnapshotValidationError("snapshot contains a duplicate business key")
    if keys != sorted(keys):
        raise SnapshotValidationError("snapshot rows are not in canonical order")
    if payload != canonical_snapshot_bytes(rows):
        raise SnapshotValidationError("snapshot bytes are not canonical")

    manifest = _load_manifest(manifest_path)
    response_total = sum(row.successful_response_records for row in rows)
    token_total = sum(row.success_token_count for row in rows)
    digest = hashlib.sha256(payload).hexdigest()
    actual = (len(rows), response_total, token_total, digest)
    declared = (
        manifest.row_count,
        manifest.successful_response_records,
        manifest.success_token_count,
        manifest.metrics_sha256,
    )
    if actual != declared:
        raise SnapshotValidationError(
            f"manifest mismatch: declared={declared!r}, actual={actual!r}"
        )
    _validate_expectations(expectations, actual)
    return ValidatedSnapshot(
        rows=rows,
        manifest=manifest,
        row_count=len(rows),
        successful_response_records=response_total,
        success_token_count=token_total,
        metrics_sha256=digest,
    )


def _parse_row(values: list[str], line_number: int) -> MetricRow:
    if len(values) != len(SNAPSHOT_HEADER):
        raise SnapshotValidationError(
            f"line {line_number}: expected {len(SNAPSHOT_HEADER)} columns"
        )
    try:
        stat_hour = int(values[1])
        responses = int(values[3])
        tokens = int(values[4])
    except ValueError as exc:
        raise SnapshotValidationError(
            f"line {line_number}: metrics and stat_hour must be integer values"
        ) from exc
    row = MetricRow(values[0], stat_hour, values[2], responses, tokens)
    _validate_row(row, line_number)
    return row


def _validate_row(row: MetricRow, line_number: int | None = None) -> None:
    prefix = f"line {line_number}: " if line_number is not None else ""
    try:
        parsed_date = date.fromisoformat(row.stat_date)
    except (TypeError, ValueError) as exc:
        raise SnapshotValidationError(f"{prefix}invalid stat_date") from exc
    if parsed_date.isoformat() != row.stat_date:
        raise SnapshotValidationError(f"{prefix}stat_date must be canonical ISO date")
    if type(row.stat_hour) is not int or not 0 <= row.stat_hour <= 23:
        raise SnapshotValidationError(f"{prefix}stat_hour must be between 0 and 23")
    if not _AIRLINE_CODE.fullmatch(row.airline_code):
        raise SnapshotValidationError(f"{prefix}invalid airline_code")
    metrics = (row.successful_response_records, row.success_token_count)
    if any(type(value) is not int for value in metrics):
        raise SnapshotValidationError(f"{prefix}metrics must be integer values")
    if any(value < 0 for value in metrics):
        raise SnapshotValidationError(f"{prefix}metrics must be nonnegative")
    if row.success_token_count < row.successful_response_records:
        raise SnapshotValidationError(
            f"{prefix}success token count is below response record count"
        )


def _load_manifest(path: Path) -> SnapshotManifest:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        manifest = SnapshotManifest(
            schema_version=raw["schema_version"],
            source_hdfs_root=raw["source_hdfs_root"],
            output_version=raw["output_version"],
            row_count=raw["row_count"],
            successful_response_records=raw["successful_response_records"],
            success_token_count=raw["success_token_count"],
            metrics_sha256=raw["metrics_sha256"],
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise SnapshotValidationError(f"invalid snapshot manifest: {exc}") from exc
    if manifest.schema_version != 1:
        raise SnapshotValidationError("invalid snapshot manifest schema_version")
    if not manifest.source_hdfs_root.strip() or not manifest.output_version.strip():
        raise SnapshotValidationError("invalid snapshot manifest source identity")
    integer_fields = (
        manifest.row_count,
        manifest.successful_response_records,
        manifest.success_token_count,
    )
    if any(type(value) is not int or value < 0 for value in integer_fields):
        raise SnapshotValidationError("invalid snapshot manifest totals")
    if not _SHA256.fullmatch(manifest.metrics_sha256):
        raise SnapshotValidationError("invalid snapshot manifest SHA-256")
    return manifest


def _validate_expectations(
    expectations: SnapshotExpectations | None,
    actual: tuple[int, int, int, str],
) -> None:
    if expectations is None:
        return
    expected = (
        expectations.row_count,
        expectations.successful_response_records,
        expectations.success_token_count,
        expectations.metrics_sha256,
    )
    for wanted, observed in zip(expected, actual, strict=True):
        if wanted is not None and wanted != observed:
            raise SnapshotValidationError(
                f"snapshot expectation mismatch: expected {wanted!r}, got {observed!r}"
            )
