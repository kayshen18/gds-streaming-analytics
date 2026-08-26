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
