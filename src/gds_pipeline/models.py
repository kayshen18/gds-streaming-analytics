"""Typed records shared by parsing and profiling."""

from dataclasses import dataclass
from enum import Enum


class ParseStatus(str, Enum):
    """Outcome of parsing one physical source line."""

    VALID = "valid"
    INVALID = "invalid"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True, slots=True)
class ParsedRecord:
    """Normalized representation of one source line."""

    line_number: int
    raw_line: str
    group_id: str | None
    log_type: str | None
    event_date: str | None
    event_hour: int | None
    event_time: str | None
    success_tokens: tuple[str, ...]
    parse_status: ParseStatus
    failure_reason: str | None
