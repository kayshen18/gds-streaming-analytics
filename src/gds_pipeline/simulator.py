"""Generate valid synthetic GDS records for streaming demonstrations."""

from collections.abc import Callable
from datetime import datetime, timezone
import hashlib
import random
import time

from .event import build_event
from .kafka_config import ProducerSettings
from .producer import (
    ConfluentProducerAdapter,
    DeliveryTracker,
    ProducerAdapter,
    ProductionSummary,
)


SUPPORTED_LOG_TYPES = frozenset({"ITAREQ", "ITARES"})

class SimulatedRecordGenerator:
    """Generate an unbounded reproducible sequence of valid GDS records."""

    def __init__(
        self,
        *,
        airline_codes: tuple[str, ...],
        seed: int,
        clock: Callable[[], datetime] | None = None,
        response_probability: float = 0.5,
        success_probability: float = 0.9,
    ) -> None:
        if not airline_codes:
            raise ValueError(
                "airline_codes must contain at least one code"
            )
        if not 0 <= response_probability <= 1:
            raise ValueError(
                "response_probability must be between 0 and 1"
            )
        if not 0 <= success_probability <= 1:
            raise ValueError(
                "success_probability must be between 0 and 1"
            )

        normalized_codes = tuple(
            code.strip().upper()
            for code in airline_codes
        )
        for code in normalized_codes:
            if len(code) != 2 or not code.isalnum():
                raise ValueError(
                    "each airline code must contain "
                    "two letters or digits"
                )

        self._airline_codes = normalized_codes
        self._clock = clock or (
            lambda: datetime.now(timezone.utc)
        )
        self._response_probability = response_probability
        self._success_probability = success_probability
        self._random = random.Random(seed)
        self._sequence = 0

    def next_line(self) -> str:
        """Return the next synthetic record in the sequence."""

        self._sequence += 1
        airline_code = self._random.choice(
            self._airline_codes
        )
        is_response = (
            self._random.random()
            < self._response_probability
        )
        successful = (
            self._random.random()
            < self._success_probability
        )

        return build_simulated_line(
            sequence=self._sequence,
            observed_at=self._clock(),
            airline_code=airline_code,
            log_type="ITARES" if is_response else "ITAREQ",
            successful=successful,
        )


def build_simulated_line(
    *,
    sequence: int,
    observed_at: datetime,
    airline_code: str,
    log_type: str,
    successful: bool,
) -> str:
    """Build one parser-compatible synthetic GDS source line."""

    if sequence < 1:
        raise ValueError("sequence must be at least 1")
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must include timezone information")

    normalized_airline = airline_code.strip().upper()
    if len(normalized_airline) != 2 or not normalized_airline.isalnum():
        raise ValueError("airline_code must contain two letters or digits")

    normalized_log_type = log_type.strip().upper()
    if normalized_log_type not in SUPPORTED_LOG_TYPES:
        raise ValueError(
            "log_type must be either ITAREQ or ITARES"
        )

    timestamp = observed_at.astimezone(timezone.utc)
    group_id = f"SIM.P{sequence:08d}"
    event_date = timestamp.strftime("%Y%m%d")
    event_hour = timestamp.hour
    event_time = (
        timestamp.strftime("%H:%M:%S:")
        + f"{timestamp.microsecond // 1000:03d}"
    )

    if normalized_log_type == "ITAREQ":
        return (
            f"{group_id},ITAREQ,{event_date},{event_hour},"
            f"{event_time},payload,{normalized_airline};,,"
        )

    response_status = "success" if successful else "fail"
    return (
        f"{group_id},ITARES,{event_date},{event_hour},"
        f"{event_time},,,1,"
        f"{normalized_airline}:{response_status};"
    )


def simulate_to_kafka(
    settings: ProducerSettings,
    *,
    generator: SimulatedRecordGenerator,
    run_id: str,
    producer: ProducerAdapter | None = None,
) -> ProductionSummary:
    """Publish synthetic records until the limit or an interruption."""

    normalized_run_id = run_id.strip()
    if not normalized_run_id:
        raise ValueError("run_id must not be blank")

    started_at = time.monotonic()
    source_file = f"gds-simulator:{normalized_run_id}"
    source_sha256 = hashlib.sha256(
        normalized_run_id.encode("utf-8")
    ).hexdigest()
    adapter = producer or ConfluentProducerAdapter(settings)
    tracker = DeliveryTracker()
    submitted = 0
    interrupted = False

    def delivery_callback(
        line_number: int,
    ) -> Callable[[str | None], None]:
        def delivered(error: str | None) -> None:
            if error is None:
                tracker.mark_success(line_number)
            else:
                tracker.mark_failure(line_number, error)

        return delivered

    try:
        while (
            settings.limit is None
            or submitted < settings.limit
        ):
            line_number = submitted + 1
            raw_line = generator.next_line()
            event = build_event(
                source_file=source_file,
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
                        on_delivery=delivery_callback(
                            line_number
                        ),
                    )
                    break
                except BufferError:
                    adapter.poll(0.1)

            submitted += 1
            adapter.poll(0)

            if settings.rate is not None:
                target = (
                    started_at
                    + submitted / settings.rate
                )
                delay = target - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
    except KeyboardInterrupt:
        interrupted = True

    remaining = adapter.flush(settings.flush_timeout)

    return ProductionSummary(
        submitted=submitted,
        acknowledged=tracker.success_count,
        failed=len(tracker.failures),
        last_contiguous_confirmed_line=(
            tracker.contiguous_confirmed_line
        ),
        remaining_after_flush=remaining,
        interrupted=interrupted,
        elapsed_seconds=time.monotonic() - started_at,
    )
