import csv
import json
from pathlib import Path

import pytest

from gds_pipeline.profiler import profile_file, write_artifacts


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


def test_writes_four_reconciled_deterministic_artifacts(
    tmp_path: Path,
) -> None:
    result = profile_file(FIXTURE)
    output_dir = tmp_path / "baseline"

    paths = write_artifacts(result, output_dir)

    assert {path.name for path in paths} == {
        "profile.json",
        "hourly_airline_metrics.csv",
        "invalid_records.csv",
        "duplicate_summary.json",
    }
    profile = json.loads((output_dir / "profile.json").read_text("utf-8"))
    with (output_dir / "hourly_airline_metrics.csv").open(
        encoding="utf-8", newline=""
    ) as source:
        rows = list(csv.DictReader(source))
    assert rows == sorted(
        rows,
        key=lambda row: (
            row["stat_date"],
            int(row["stat_hour"]),
            row["airline_code"],
        ),
    )
    assert sum(int(row["successful_booking_tokens"]) for row in rows) == (
        profile["total_success_tokens"]
    )

    with pytest.raises(FileExistsError):
        write_artifacts(result, output_dir)


def test_overwrite_replaces_an_existing_output_directory(
    tmp_path: Path,
) -> None:
    result = profile_file(FIXTURE)
    output_dir = tmp_path / "baseline"
    write_artifacts(result, output_dir)
    (output_dir / "stale.txt").write_text("stale", encoding="utf-8")

    write_artifacts(result, output_dir, overwrite=True)

    assert not (output_dir / "stale.txt").exists()
