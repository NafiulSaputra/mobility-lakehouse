from __future__ import annotations

from pathlib import Path

import pytest

from mobility_lakehouse.databricks_job import build_parser as job_parser
from mobility_lakehouse.delta_io import Table
from mobility_lakehouse.pipeline import (
    GoldSummary,
    catalog_layout,
    gold_summary,
    local_layout,
    silver_summary,
    snapshot_summary,
)
from mobility_lakehouse.silver import RuleCount, SilverResult
from mobility_lakehouse.tlc import Month

JAN = Month.parse("2025-01")


def test_table_needs_exactly_one_location() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        Table()
    with pytest.raises(ValueError, match="exactly one"):
        Table(path="/data/t", name="c.s.t")
    assert str(Table(name="c.s.t")) == "c.s.t"


def test_local_layout_uses_folders() -> None:
    layout = local_layout(Path("data"), Path("data/lakehouse"))

    assert layout.raw_file(JAN) == "data/raw/fhvhv/fhvhv_tripdata_2025-01.parquet"
    assert layout.zones_file == "data/raw/reference/taxi_zone_lookup.csv"
    assert layout.bronze == Table(path="data/lakehouse/bronze/fhvhv_trips")
    assert layout.gold.daily_company_trips == Table(path="data/lakehouse/gold/fhvhv_daily_company_trips")
    assert layout.snapshot_dir == "data/snapshot"
    assert layout.persist is True


def test_catalog_layout_uses_unity_catalog_tables_and_a_volume() -> None:
    layout = catalog_layout("workspace", "mobility")

    assert layout.raw_file(JAN) == "/Volumes/workspace/mobility/raw/fhvhv/fhvhv_tripdata_2025-01.parquet"
    assert layout.zones_file == "/Volumes/workspace/mobility/raw/reference/taxi_zone_lookup.csv"
    assert layout.bronze == Table(name="workspace.mobility.bronze_fhvhv_trips")
    assert layout.silver.quarantine == Table(name="workspace.mobility.quarantine_fhvhv_trips")
    assert layout.gold.monthly_driver_economics == Table(
        name="workspace.mobility.gold_fhvhv_monthly_driver_economics"
    )
    assert layout.snapshot_dir == "/Volumes/workspace/mobility/serving/snapshot"
    # Databricks serverless does not support DataFrame caching (ADR 0009).
    assert layout.persist is False


def test_job_arguments() -> None:
    args = job_parser().parse_args(["silver", "--month", "2024-06", "--catalog", "main", "--schema", "taxi"])

    assert (args.step, str(args.month), args.catalog, args.schema, args.volume, args.serving_volume) == (
        "silver",
        "2024-06",
        "main",
        "taxi",
        "raw",
        "serving",
    )


def test_job_has_a_snapshot_step() -> None:
    assert job_parser().parse_args(["snapshot", "--month", "2025-03"]).step == "snapshot"


def test_job_rejects_unknown_steps() -> None:
    with pytest.raises(SystemExit):
        job_parser().parse_args(["platinum", "--month", "2025-01"])


def test_silver_summary_lists_every_rule() -> None:
    result = SilverResult(
        total_rows=1000,
        silver_rows=990,
        quarantined_rows=10,
        rule_counts=(RuleCount("Q004", "reject", "Negative base fare", 10),),
    )
    lines = silver_summary(JAN, result)

    assert lines[0] == "Silver 2025-01: 1,000 bronze rows"
    assert "990 (99.0000%)" in lines[1]
    assert lines[3].strip().startswith("Q004 Negative base fare")


def test_snapshot_summary_lists_every_table() -> None:
    lines = snapshot_summary(JAN, {"quality_months": 1, "hourly_pickup_zones": 187_018}, "data/snapshot")

    assert lines[0] == "Snapshot 2025-01 in data/snapshot:"
    assert "187,018 rows" in lines[2]


def test_gold_summary_shows_driver_economics() -> None:
    summary = GoldSummary(
        rows_per_table={"monthly_driver_economics": 2},
        driver_economics=[
            {
                "company": "Uber",
                "trips": 15,
                "driver_pay_per_mile": 3.8,
                "driver_pay_per_minute": 1.0,
                "driver_share_of_base_fare": 0.77,
            }
        ],
    )
    lines = gold_summary(JAN, summary)

    assert lines[0] == "Gold 2025-01:"
    assert "$3.8/mile" in lines[-1]
