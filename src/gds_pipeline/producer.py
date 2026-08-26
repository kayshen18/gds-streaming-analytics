"""Stream raw GDS records to Kafka with acknowledgement checkpoints."""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import time
from typing import Callable, Protocol

from .checkpoint import CHECKPOINT_SCHEMA_VERSION, Checkpoint, load_checkpoint
from .event import build_event
from .kafka_config import ProducerSettings


CHECKPOINT_INTERVAL = 10_000


class ProducerAdapter(Protocol):
    def produce(
        self,
        *,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Callable[[str | None], None],
    ) -> None: ...

    def poll(self, timeout: float) -> object: ...

    def flush(self, timeout: float) -> int: ...


class ConfluentProducerAdapter:
    """Small boundary around confluent-kafka's callback signature."""

    def __init__(self, settings: ProducerSettings) -> None:
        from confluent_kafka import Producer

        self._producer = Producer(settings.to_client_config())

    def produce(
        self,
        *,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Callable[[str | None], None],
    ) -> None:
        self._producer.produce(
            topic=topic,
            key=key,
            value=value,
            on_delivery=lambda error, _message: on_delivery(
                str(error) if error is not None else None
            ),
        )

    def poll(self, timeout: float) -> object:
        return self._producer.poll(timeout)

    def flush(self, timeout: float) -> int:
        return self._producer.flush(timeout)


@dataclass(frozen=True, slots=True)
class ProductionSummary:
    submitted: int
    acknowledged: int
    failed: int
    last_contiguous_confirmed_line: int
    remaining_after_flush: int
    interrupted: bool
    elapsed_seconds: float


class DeliveryTracker:
    """Advance only through a contiguous sequence of acknowledged lines."""

    def __init__(self, contiguous_confirmed_line: int = 0) -> None:
        if contiguous_confirmed_line < 0:
            raise ValueError("contiguous_confirmed_line must not be negative")
        self._contiguous_confirmed_line = contiguous_confirmed_line
        self._pending_successes: set[int] = set()
        self._failures: dict[int, str] = {}
        self._success_count = 0

    @property
    def contiguous_confirmed_line(self) -> int:
        return self._contiguous_confirmed_line

    @property
    def failures(self) -> dict[int, str]:
        return dict(self._failures)

    @property
    def success_count(self) -> int:
        return self._success_count

    def mark_success(self, line_number: int) -> None:
        """Record an acknowledgement and advance over any closed gap."""

        if line_number <= self._contiguous_confirmed_line:
            return
        if line_number in self._pending_successes:
            return
        self._pending_successes.add(line_number)
        self._success_count += 1
        next_line = self._contiguous_confirmed_line + 1
        while next_line in self._pending_successes:
            self._pending_successes.remove(next_line)
            self._contiguous_confirmed_line = next_line
            next_line += 1

    def mark_failure(self, line_number: int, error: str) -> None:
        """Record a failed delivery without advancing the checkpoint."""

        self._failures[line_number] = error


def produce_file(
    settings: ProducerSettings,
    *,
    producer: ProducerAdapter | None = None,
    progress: Callable[[int], None] | None = None,
) -> ProductionSummary:
    """Publish physical source lines and checkpoint confirmed progress."""

    if settings.input_path is None:
        raise ValueError("input_path is required")
    input_path = Path(settings.input_path)
    if not input_path.is_file():
        raise ValueError(f"input_path is not a file: {input_path}")

    started_at = time.monotonic()
    source_sha256 = _sha256_file(input_path)
    checkpoint = (
        load_checkpoint(
            Path(settings.checkpoint_path),
            source_sha256=source_sha256,
            topic=settings.topic,
            reset=settings.reset_checkpoint,
        )
        if settings.checkpoint_path is not None
        else None
    )
    confirmed_at_start = (
        checkpoint.last_contiguous_confirmed_line if checkpoint else 0
    )
    tracker = DeliveryTracker(confirmed_at_start)
    adapter = producer or ConfluentProducerAdapter(settings)
    report_progress = progress or (
        lambda count: print(f"Submitted {count:,} records")
    )
    submitted = 0
    interrupted = False
    last_saved_line = confirmed_at_start

    def save_progress(*, force: bool = False) -> None:
        nonlocal last_saved_line
        if settings.checkpoint_path is None:
            return
        confirmed = tracker.contiguous_confirmed_line
        if not force and confirmed - last_saved_line < CHECKPOINT_INTERVAL:
            return
        _checkpoint_for(
            source_sha256=source_sha256,
            confirmed_line=confirmed,
            topic=settings.topic,
        ).save_atomic(Path(settings.checkpoint_path))
        last_saved_line = confirmed

    def delivery_callback(line_number: int) -> Callable[[str | None], None]:
        def delivered(error: str | None) -> None:
            if error is None:
                tracker.mark_success(line_number)
                save_progress()
            else:
                tracker.mark_failure(line_number, error)

        return delivered

    try:
        with input_path.open("r", encoding="utf-8", newline="") as source:
            for line_number, line in enumerate(source, start=1):
                if line_number <= confirmed_at_start:
                    continue
                if settings.limit is not None and submitted >= settings.limit:
                    break
                raw_line = line.rstrip("\r\n")
                event = build_event(
                    source_file=input_path.name,
                    source_sha256=source_sha256,
                    line_number=line_number,
                    raw_line=raw_line,
                )
                while True:
                    try:
                        adapter.produce(
                            topic=settings.topic,
                            key=event.kafka_key(),
                            value=event.to_json_bytes(),
                            on_delivery=delivery_callback(line_number),
                        )
                        break
                    except BufferError:
                        adapter.poll(0.1)
                submitted += 1
                adapter.poll(0)
                if submitted % 10_000 == 0:
                    report_progress(submitted)
                if settings.rate is not None:
                    target = started_at + submitted / settings.rate
                    delay = target - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
    except KeyboardInterrupt:
        interrupted = True

    remaining = adapter.flush(settings.flush_timeout)
    save_progress(force=True)
    return ProductionSummary(
        submitted=submitted,
        acknowledged=tracker.success_count,
        failed=len(tracker.failures),
        last_contiguous_confirmed_line=tracker.contiguous_confirmed_line,
        remaining_after_flush=remaining,
        interrupted=interrupted,
        elapsed_seconds=time.monotonic() - started_at,
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _checkpoint_for(
    *, source_sha256: str, confirmed_line: int, topic: str
) -> Checkpoint:
    updated_at = datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")
    return Checkpoint(
        schema_version=CHECKPOINT_SCHEMA_VERSION,
        source_sha256=source_sha256,
        last_contiguous_confirmed_line=confirmed_line,
        topic=topic,
        updated_at=updated_at,
    )
