"""Pure parsing functions for GDS source records."""

from datetime import datetime
import re

from .models import ParsedRecord, ParseStatus


SUCCESS_TOKEN = re.compile(
    r"(?<![A-Z0-9])([A-Z0-9]{2}):success(?![A-Za-z])"
)


def parse_line(line: str, line_number: int) -> ParsedRecord:
    """Parse one line without raising for data-quality problems."""

    raw_line = line.rstrip("\r\n")
    if not raw_line.strip():
        return _rejected(line_number, raw_line, "blank_line")

    fields = raw_line.split(",")
    if len(fields) < 5:
        return _rejected(line_number, raw_line, "too_few_fields")

    group_id = fields[0].strip()
    log_type = fields[1].strip()
    event_date = fields[2].strip()
    event_time = fields[4].strip()

    if not group_id:
        return _rejected(
            line_number, raw_line, "missing_group_id", log_type=log_type
        )
    if not log_type:
        return _rejected(
            line_number, raw_line, "missing_log_type", group_id=group_id
        )
    try:
        datetime.strptime(event_date, "%Y%m%d")
    except ValueError:
        return _rejected(
            line_number,
            raw_line,
            "invalid_date",
            group_id=group_id,
            log_type=log_type,
        )
    try:
        event_hour = int(fields[3].strip())
    except ValueError:
        return _rejected(
            line_number,
            raw_line,
            "invalid_hour",
            group_id=group_id,
            log_type=log_type,
            event_date=event_date,
        )
    if not 0 <= event_hour <= 23:
        return _rejected(
            line_number,
            raw_line,
            "invalid_hour",
            group_id=group_id,
            log_type=log_type,
            event_date=event_date,
        )
    if log_type not in {"ITARES", "ITAREQ"}:
        return ParsedRecord(
            line_number=line_number,
            raw_line=raw_line,
            group_id=group_id,
            log_type=log_type,
            event_date=event_date,
            event_hour=event_hour,
            event_time=event_time,
            success_tokens=(),
            parse_status=ParseStatus.UNSUPPORTED,
            failure_reason="unsupported_log_type",
        )

    success_tokens = (
        tuple(SUCCESS_TOKEN.findall(raw_line))
        if log_type == "ITARES"
        else ()
    )
    return ParsedRecord(
        line_number=line_number,
        raw_line=raw_line,
        group_id=group_id,
        log_type=log_type,
        event_date=event_date,
        event_hour=event_hour,
        event_time=event_time,
        success_tokens=success_tokens,
        parse_status=ParseStatus.VALID,
        failure_reason=None,
    )


def _rejected(
    line_number: int,
    raw_line: str,
    reason: str,
    *,
    group_id: str | None = None,
    log_type: str | None = None,
    event_date: str | None = None,
) -> ParsedRecord:
    return ParsedRecord(
        line_number=line_number,
        raw_line=raw_line,
        group_id=group_id,
        log_type=log_type,
        event_date=event_date,
        event_hour=None,
        event_time=None,
        success_tokens=(),
        parse_status=ParseStatus.INVALID,
        failure_reason=reason,
    )
