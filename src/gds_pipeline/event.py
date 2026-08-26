"""Versioned Kafka event envelope for raw GDS source records."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json


SCHEMA_VERSION = 1


def event_id_for(source_sha256: str, line_number: int, raw_line: str) -> str:
    """Return a stable identity for one physical source record."""

    canonical = f"{SCHEMA_VERSION}\n{source_sha256}\n{line_number}\n{raw_line}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class GdsEvent:
    """Serializable event contract published to ``gds.raw.v1``."""

    schema_version: int
    event_id: str
    source_file: str
    source_file_sha256: str
    source_line_number: int
    group_id: str | None
    raw_line: str
    produced_at: str

    def to_json_bytes(self) -> bytes:
        """Serialize as deterministic, compact UTF-8 JSON."""

        return json.dumps(
            asdict(self),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    def kafka_key(self) -> bytes:
        """Keep a source group ordered, with a stable fallback for bad data."""

        return (self.group_id or self.event_id).encode("utf-8")


def build_event(
    *,
    source_file: str,
    source_sha256: str,
    line_number: int,
    raw_line: str,
) -> GdsEvent:
    """Construct an event from one source line."""

    first_field = raw_line.split(",", 1)[0].strip()
    group_id = first_field or None
    produced_at = datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")
    return GdsEvent(
        schema_version=SCHEMA_VERSION,
        event_id=event_id_for(source_sha256, line_number, raw_line),
        source_file=source_file,
        source_file_sha256=source_sha256,
        source_line_number=line_number,
        group_id=group_id,
        raw_line=raw_line,
        produced_at=produced_at,
    )
