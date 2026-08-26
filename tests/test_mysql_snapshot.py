import hashlib
import json
from pathlib import Path

import pytest

from gds_pipeline.mysql_snapshot import (
    MetricRow,
    SnapshotExpectations,
    SnapshotValidationError,
    canonical_snapshot_bytes,
    load_snapshot,
)


HEADER = (
    b"stat_date,stat_hour,airline_code,successful_response_records,"
    b"successful_booking_tokens\n"
)


def _rows() -> tuple[MetricRow, ...]:
    return (
        MetricRow("2016-06-02", 0, "MU", 2, 3),
        MetricRow("2016-06-01", 10, "CA", 4, 7),
        MetricRow("2016-06-01", 2, "CA", 1, 1),
    )


def _write_snapshot(
    directory: Path,
    csv_bytes: bytes,
    *,
    row_count: int,
    response_total: int,
    token_total: int,
    digest: str | None = None,
) -> tuple[Path, Path]:
    csv_path = directory / "metrics.csv"
    manifest_path = directory / "manifest.json"
    csv_path.write_bytes(csv_bytes)
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source_hdfs_root": "hdfs://hdfs-namenode:8020/data/gds",
                "output_version": "v1",
                "row_count": row_count,
                "successful_response_records": response_total,
                "success_token_count": token_total,
                "metrics_sha256": digest or hashlib.sha256(csv_bytes).hexdigest(),
            }
        ),
        encoding="utf-8",
    )
    return csv_path, manifest_path


def test_canonical_bytes_use_exact_header_numeric_hour_sort_and_lf() -> None:
    actual = canonical_snapshot_bytes(_rows())

    assert actual == HEADER + (
        b"2016-06-01,2,CA,1,1\n"
        b"2016-06-01,10,CA,4,7\n"
        b"2016-06-02,0,MU,2,3\n"
    )
    assert hashlib.sha256(actual).hexdigest() == (
        "67b8f10790a5f09376e1a0fdc22b2b86bf4ac09c445ba57d3b42c32c6ddb817f"
    )


def test_load_snapshot_validates_and_returns_totals(tmp_path: Path) -> None:
    payload = canonical_snapshot_bytes(_rows())
    csv_path, manifest_path = _write_snapshot(
        tmp_path,
        payload,
        row_count=3,
        response_total=7,
        token_total=11,
    )

    snapshot = load_snapshot(csv_path, manifest_path)

    assert snapshot.rows == tuple(sorted(_rows(), key=lambda row: row.key))
    assert snapshot.row_count == 3
    assert snapshot.successful_response_records == 7
    assert snapshot.success_token_count == 11
    assert snapshot.metrics_sha256 == hashlib.sha256(payload).hexdigest()
    assert snapshot.manifest.source_hdfs_root.endswith("/data/gds")


@pytest.mark.parametrize(
    ("record", "message"),
    [
        ("2016-02-30,1,CA,1,1\n", "stat_date"),
        ("2016-06-01,24,CA,1,1\n", "stat_hour"),
        ("2016-06-01,1,,1,1\n", "airline_code"),
        ("2016-06-01,1,TOO-LONG-1,1,1\n", "airline_code"),
        ("2016-06-01,1,CA,-1,1\n", "nonnegative"),
        ("2016-06-01,1,CA,one,1\n", "integer"),
        ("2016-06-01,1,CA,2,1\n", "below"),
    ],
)
def test_load_snapshot_rejects_invalid_metric_rows(
    tmp_path: Path, record: str, message: str
) -> None:
    payload = HEADER + record.encode("utf-8")
    paths = _write_snapshot(
        tmp_path, payload, row_count=1, response_total=1, token_total=1
    )

    with pytest.raises(SnapshotValidationError, match=message):
        load_snapshot(*paths)


def test_load_snapshot_rejects_wrong_header_and_noncanonical_order(
    tmp_path: Path,
) -> None:
    wrong_header = HEADER.replace(b"airline_code", b"carrier")
    paths = _write_snapshot(
        tmp_path, wrong_header + b"2016-06-01,1,CA,1,1\n",
        row_count=1, response_total=1, token_total=1,
    )
    with pytest.raises(SnapshotValidationError, match="header"):
        load_snapshot(*paths)

    unsorted = HEADER + b"2016-06-01,10,CA,1,1\n2016-06-01,2,CA,1,1\n"
    paths = _write_snapshot(
        tmp_path, unsorted, row_count=2, response_total=2, token_total=2
    )
    with pytest.raises(SnapshotValidationError, match="canonical"):
        load_snapshot(*paths)


def test_load_snapshot_rejects_duplicate_keys(tmp_path: Path) -> None:
    payload = HEADER + b"2016-06-01,1,CA,1,1\n2016-06-01,1,CA,2,2\n"
    paths = _write_snapshot(
        tmp_path, payload, row_count=2, response_total=3, token_total=3
    )

    with pytest.raises(SnapshotValidationError, match="duplicate"):
        load_snapshot(*paths)


@pytest.mark.parametrize(
    "overrides",
    [
        {"row_count": 99},
        {"response_total": 99},
        {"token_total": 99},
        {"digest": "0" * 64},
    ],
)
def test_load_snapshot_rejects_manifest_mismatch(
    tmp_path: Path, overrides: dict[str, int | str]
) -> None:
    payload = canonical_snapshot_bytes(_rows())
    values: dict[str, int | str | None] = {
        "row_count": 3,
        "response_total": 7,
        "token_total": 11,
        "digest": None,
    }
    values.update(overrides)
    paths = _write_snapshot(tmp_path, payload, **values)  # type: ignore[arg-type]

    with pytest.raises(SnapshotValidationError, match="manifest"):
        load_snapshot(*paths)


def test_load_snapshot_rejects_optional_acceptance_mismatch(tmp_path: Path) -> None:
    payload = canonical_snapshot_bytes(_rows())
    paths = _write_snapshot(
        tmp_path, payload, row_count=3, response_total=7, token_total=11
    )
    expectations = SnapshotExpectations(row_count=3203)

    with pytest.raises(SnapshotValidationError, match="expectation"):
        load_snapshot(*paths, expectations=expectations)


def test_load_snapshot_rejects_empty_snapshot(tmp_path: Path) -> None:
    paths = _write_snapshot(
        tmp_path, HEADER, row_count=0, response_total=0, token_total=0
    )

    with pytest.raises(SnapshotValidationError, match="at least one"):
        load_snapshot(*paths)
