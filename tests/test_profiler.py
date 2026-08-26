from pathlib import Path

from gds_pipeline.profiler import profile_file


FIXTURE = Path(__file__).parent / "fixtures" / "mixed_records.txt"


def test_profiles_records_without_deduplicating_primary_counts() -> None:
    result = profile_file(FIXTURE)

    assert result.physical_line_count == 6
    assert result.blank_line_count == 1
    assert result.log_type_counts == {"ITARES": 3, "ITAREQ": 1}
    assert result.invalid_count == 2
    assert result.unsupported_count == 0
    assert result.duplicate_record_count == 1
    assert result.duplicate_group_count == 1

    metric = result.metrics[("20180830", 19, "CA")]
    assert metric.response_records == 3
    assert metric.booking_tokens == 4


def test_invalid_retention_limit_does_not_change_invalid_count(
    tmp_path: Path,
) -> None:
    source = tmp_path / "invalid.txt"
    source.write_text("\n".join(["bad"] * 5) + "\n", encoding="utf-8")

    result = profile_file(source, invalid_limit=2)

    assert result.physical_line_count == 5
    assert result.invalid_count == 5
    assert len(result.invalid_records) == 2
