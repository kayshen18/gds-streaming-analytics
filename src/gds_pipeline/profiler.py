"""Streaming profiling and aggregation for GDS log files."""

from collections import Counter, defaultdict
from dataclasses import dataclass, field
import hashlib
from pathlib import Path
from time import perf_counter

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
