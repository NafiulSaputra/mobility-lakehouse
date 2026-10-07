from __future__ import annotations

from pathlib import Path

from mobility_lakehouse.profiling import DuplicateStats, Profile, render_markdown, write_report


def make_profile() -> Profile:
    return Profile(
        dataset="fhvhv",
        month="2025-01",
        source_file="fhvhv_tripdata_2025-01.parquet",
        generated_at="2026-10-07 09:00 UTC",
        row_count=1_000,
        schema=[("pickup_datetime", "timestamp_ntz"), ("trip_miles", "double")],
        nulls={"pickup_datetime": 0, "trip_miles": 5},
        checks={"pickup_outside_file_month": 10, "dropoff_before_pickup": 0},
        pickup_range=("2024-12-31 23:59:00", "2025-02-01 00:10:00"),
        numeric={
            "trip_miles": {"min": -1.0, "p50": 3.2, "p99": 25.0, "max": 400.0, "negative": 2, "zero": 7},
        },
        zones_out_of_range={"PULocationID": 0},
        flags={"shared_request_flag": [("N", 990), ("Y", 10)]},
        full_row_duplicates=DuplicateStats(groups=3, rows=7),
        candidate_key_duplicates=DuplicateStats(groups=0, rows=0),
    )


def test_duplicate_stats_extra_rows() -> None:
    assert DuplicateStats(groups=3, rows=7).extra_rows == 4


def test_report_contains_key_findings() -> None:
    report = render_markdown(make_profile())

    assert report.startswith("# Profiling report: fhvhv 2025-01")
    assert "Rows: **1,000**" in report
    # 4 extra rows out of 1,000 rows
    assert "4 extra rows, 0.4000% of all rows" in report
    assert "| Pickup time outside the file's month | 10 | 1.0000% |" in report
    assert "| `trip_miles` | -1.00 | 3.20 | 25.00 | 400.00 | 2 (0.2000%) | 7 (0.7000%) |" in report
    assert "`N`: 990" in report


def test_report_handles_empty_month() -> None:
    profile = make_profile()
    profile.row_count = 0

    assert "n/a" in render_markdown(profile)


def test_write_report_uses_dataset_and_month_in_the_name(tmp_path: Path) -> None:
    path = write_report(make_profile(), tmp_path / "profiling")

    assert path == tmp_path / "profiling" / "fhvhv_2025-01.md"
    assert path.read_text(encoding="utf-8").endswith("\n")
