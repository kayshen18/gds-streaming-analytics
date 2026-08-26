import pytest

from gds_pipeline.models import ParseStatus
from gds_pipeline.parser import parse_line


def test_parses_itares_with_repeated_success_tokens() -> None:
    line = (
        "TB.P1780,ITARES,20180830,19,19:45:36:257,,,1,"
        "CA:success;CA:success;"
    )

    record = parse_line(line, 7)

    assert record.line_number == 7
    assert record.group_id == "TB.P1780"
    assert record.log_type == "ITARES"
    assert record.event_date == "20180830"
    assert record.event_hour == 19
    assert record.event_time == "19:45:36:257"
    assert record.success_tokens == ("CA", "CA")
    assert record.parse_status is ParseStatus.VALID
    assert record.failure_reason is None


def test_parses_valid_itareq_without_success_tokens() -> None:
    line = (
        "VA.P2241,ITAREQ,20180830,19,19:45:36:647,"
        "payload,CA;CA;,,"
    )

    record = parse_line(line, 2)

    assert record.log_type == "ITAREQ"
    assert record.success_tokens == ()
    assert record.parse_status is ParseStatus.VALID


@pytest.mark.parametrize(
    ("line", "status", "reason"),
    [
        ("\n", ParseStatus.INVALID, "blank_line"),
        (
            "A,ITARES,20180830",
            ParseStatus.INVALID,
            "too_few_fields",
        ),
        (
            ",ITARES,20180830,19,19:00:00:000",
            ParseStatus.INVALID,
            "missing_group_id",
        ),
        (
            "A,,20180830,19,19:00:00:000",
            ParseStatus.INVALID,
            "missing_log_type",
        ),
        (
            "A,ITARES,20181340,19,19:00:00:000",
            ParseStatus.INVALID,
            "invalid_date",
        ),
        (
            "A,ITARES,20180830,24,19:00:00:000",
            ParseStatus.INVALID,
            "invalid_hour",
        ),
        (
            "A,OTHER,20180830,19,19:00:00:000",
            ParseStatus.UNSUPPORTED,
            "unsupported_log_type",
        ),
    ],
)
def test_classifies_non_valid_records(
    line: str,
    status: ParseStatus,
    reason: str,
) -> None:
    record = parse_line(line, 1)

    assert record.parse_status is status
    assert record.failure_reason == reason


def test_itares_without_success_is_valid() -> None:
    line = "A,ITARES,20180830,19,19:00:00:000,,,1,CA:fail;"

    record = parse_line(line, 1)

    assert record.parse_status is ParseStatus.VALID
    assert record.success_tokens == ()
