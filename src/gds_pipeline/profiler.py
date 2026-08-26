"""Streaming profiling and aggregation for GDS log files."""

from collections import Counter, defaultdict
import csv
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from time import perf_counter
from uuid import uuid4

from .models import ParsedRecord, ParseStatus
from .parser import parse_line


@dataclass(slots=True)
class AirlineMetric:
    """Two deliberately distinct success measurements."""

    response_records: int = 0
    booking_tokens: int = 0


@dataclass(slots=True)
class ProfileResult:
    """In-memory summary of one complete source scan."""

    source_name: str
    byte_size: int
    sha256: str
    physical_line_count: int = 0
    blank_line_count: int = 0
    valid_count: int = 0
    invalid_count: int = 0
    unsupported_count: int = 0
    log_type_counts: Counter[str] = field(default_factory=Counter)
    failure_reason_counts: Counter[str] = field(default_factory=Counter)
    metrics: dict[tuple[str, int, str], AirlineMetric] = field(
        default_factory=dict
    )
    invalid_records: list[ParsedRecord] = field(default_factory=list)
    duplicate_record_count: int = 0
    duplicate_group_count: int = 0
    duplicate_fingerprints: Counter[str] = field(default_factory=Counter)
    earliest_date: str | None = None
    latest_date: str | None = None
    observed_hours: set[int] = field(default_factory=set)
    airline_codes: set[str] = field(default_factory=set)
    elapsed_seconds: float = 0.0

    @property
    def records_per_second(self) -> float:
        if self.elapsed_seconds == 0:
            return 0.0
        return self.physical_line_count / self.elapsed_seconds

    @property
    def total_success_tokens(self) -> int:
        return sum(metric.booking_tokens for metric in self.metrics.values())

    @property
    def successful_response_records(self) -> int:
        return sum(metric.response_records for metric in self.metrics.values())


def profile_file(path: Path, invalid_limit: int = 1000) -> ProfileResult:
    """Profile a UTF-8 GDS log without loading the file into memory."""

    if invalid_limit < 0:
        raise ValueError("invalid_limit must be nonnegative")

    started = perf_counter()
    result = ProfileResult(
        source_name=path.name,
        byte_size=path.stat().st_size,
        sha256=_sha256_file(path),
    )
    metrics: defaultdict[tuple[str, int, str], AirlineMetric] = defaultdict(
        AirlineMetric
    )
    fingerprints: Counter[str] = Counter()

    with path.open("r", encoding="utf-8", errors="strict", newline="") as source:
        for line_number, line in enumerate(source, start=1):
            result.physical_line_count += 1
            record = parse_line(line, line_number)
            fingerprint = hashlib.sha256(
                record.raw_line.encode("utf-8")
            ).hexdigest()
            fingerprints[fingerprint] += 1

            if record.log_type:
                result.log_type_counts[record.log_type] += 1

            if record.parse_status is ParseStatus.INVALID:
                result.invalid_count += 1
                if record.failure_reason == "blank_line":
                    result.blank_line_count += 1
                if record.failure_reason:
                    result.failure_reason_counts[record.failure_reason] += 1
                if len(result.invalid_records) < invalid_limit:
                    result.invalid_records.append(record)
                continue

            if record.parse_status is ParseStatus.UNSUPPORTED:
                result.unsupported_count += 1
                if record.failure_reason:
                    result.failure_reason_counts[record.failure_reason] += 1
                continue

            result.valid_count += 1
            _observe_dimensions(result, record)
            if record.log_type != "ITARES":
                continue

            assert record.event_date is not None
            assert record.event_hour is not None
            token_counts = Counter(record.success_tokens)
            for airline, token_count in token_counts.items():
                metric = metrics[
                    (record.event_date, record.event_hour, airline)
                ]
                metric.response_records += 1
                metric.booking_tokens += token_count
                result.airline_codes.add(airline)

    result.metrics = dict(metrics)
    result.duplicate_fingerprints = Counter(
        {key: count for key, count in fingerprints.items() if count > 1}
    )
    result.duplicate_record_count = sum(
        count - 1 for count in result.duplicate_fingerprints.values()
    )
    result.duplicate_group_count = len(result.duplicate_fingerprints)
    result.elapsed_seconds = perf_counter() - started
    return result


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _observe_dimensions(result: ProfileResult, record: ParsedRecord) -> None:
    if record.event_date is not None:
        if result.earliest_date is None or record.event_date < result.earliest_date:
            result.earliest_date = record.event_date
        if result.latest_date is None or record.event_date > result.latest_date:
            result.latest_date = record.event_date
    if record.event_hour is not None:
        result.observed_hours.add(record.event_hour)


def write_artifacts(
    result: ProfileResult,
    output_dir: Path,
    overwrite: bool = False,
) -> tuple[Path, Path, Path, Path]:
    """Write a complete artifact directory, replacing it only on success."""

    output_dir = output_dir.resolve()
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    if output_dir.exists() and not overwrite:
        raise FileExistsError(f"output directory already exists: {output_dir}")

    temporary = Path(
        tempfile.mkdtemp(
            prefix=f".{output_dir.name}.tmp-",
            dir=output_dir.parent,
        )
    )
    backup: Path | None = None
    try:
        _write_profile(result, temporary / "profile.json")
        _write_metrics(result, temporary / "hourly_airline_metrics.csv")
        _write_invalid(result, temporary / "invalid_records.csv")
        _write_duplicates(result, temporary / "duplicate_summary.json")

        if output_dir.exists():
            backup = output_dir.with_name(
                f".{output_dir.name}.backup-{uuid4().hex}"
            )
            os.replace(output_dir, backup)
        os.replace(temporary, output_dir)
        if backup is not None:
            shutil.rmtree(backup)
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        if backup is not None and backup.exists() and not output_dir.exists():
            os.replace(backup, output_dir)
        raise

    names = (
        "profile.json",
        "hourly_airline_metrics.csv",
        "invalid_records.csv",
        "duplicate_summary.json",
    )
    return tuple(output_dir / name for name in names)  # type: ignore[return-value]


def _write_profile(result: ProfileResult, path: Path) -> None:
    payload = {
        "source_name": result.source_name,
        "byte_size": result.byte_size,
        "sha256": result.sha256,
        "physical_line_count": result.physical_line_count,
        "blank_line_count": result.blank_line_count,
        "valid_count": result.valid_count,
        "invalid_count": result.invalid_count,
        "unsupported_count": result.unsupported_count,
        "log_type_counts": dict(sorted(result.log_type_counts.items())),
        "failure_reason_counts": dict(
            sorted(result.failure_reason_counts.items())
        ),
        "earliest_date": result.earliest_date,
        "latest_date": result.latest_date,
        "observed_hours": sorted(result.observed_hours),
        "airline_codes": sorted(result.airline_codes),
        "total_success_tokens": result.total_success_tokens,
        "successful_response_records": result.successful_response_records,
        "duplicate_record_count": result.duplicate_record_count,
        "duplicate_group_count": result.duplicate_group_count,
        "elapsed_seconds": result.elapsed_seconds,
        "records_per_second": result.records_per_second,
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


def _write_metrics(result: ProfileResult, path: Path) -> None:
    fieldnames = [
        "stat_date",
        "stat_hour",
        "airline_code",
        "successful_response_records",
        "successful_booking_tokens",
    ]
    with path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for (date, hour, airline), metric in sorted(result.metrics.items()):
            writer.writerow(
                {
                    "stat_date": date,
                    "stat_hour": hour,
                    "airline_code": airline,
                    "successful_response_records": metric.response_records,
                    "successful_booking_tokens": metric.booking_tokens,
                }
            )


def _write_invalid(result: ProfileResult, path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(
            target,
            fieldnames=["line_number", "failure_reason", "raw_line"],
            lineterminator="\n",
        )
        writer.writeheader()
        for record in result.invalid_records:
            writer.writerow(
                {
                    "line_number": record.line_number,
                    "failure_reason": record.failure_reason,
                    "raw_line": record.raw_line,
                }
            )


def _write_duplicates(result: ProfileResult, path: Path) -> None:
    repeated = sorted(
        result.duplicate_fingerprints.items(),
        key=lambda item: (-item[1], item[0]),
    )
    payload = {
        "duplicate_group_count": result.duplicate_group_count,
        "duplicate_record_count": result.duplicate_record_count,
        "fingerprints": [
            {"sha256": fingerprint, "occurrences": count}
            for fingerprint, count in repeated
        ],
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
