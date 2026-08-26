"""Pure parsing functions for GDS source records."""

from datetime import datetime
import re

from .models import ParsedRecord, ParseStatus


SUCCESS_TOKEN = re.compile(
    r"(?<![A-Z0-9])([A-Z0-9]{2}):success(?![A-Za-z])"
)


def parse_line(line: str, line_number: int) -> ParsedRecord:
    """Parse one supported, structurally valid GDS record."""

    raw_line = line.rstrip("\r\n")
    fields = raw_line.split(",")
    if len(fields) < 5:
        raise ValueError("record has fewer than five fields")

    group_id = fields[0].strip()
    log_type = fields[1].strip()
    event_date = fields[2].strip()
    event_hour = int(fields[3].strip())
    event_time = fields[4].strip()

    datetime.strptime(event_date, "%Y%m%d")
    if not 0 <= event_hour <= 23:
        raise ValueError("hour must be between 0 and 23")

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
