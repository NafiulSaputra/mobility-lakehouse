"""Queries of the API over a small snapshot (ADR 0011)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytest.importorskip("duckdb")

from snapshot_data import FEB, MAR, SNAPSHOT_ROWS, write_snapshot  # noqa: E402

from mobility_lakehouse.api.store import (  # noqa: E402
    SPIKE_FACTOR,
    SPIKE_MIN_ROWS,
    SnapshotNotFoundError,
    SnapshotStore,
)
from mobility_lakehouse.snapshot import SNAPSHOT_TABLES  # noqa: E402

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture
def store(snapshot_dir: Path) -> SnapshotStore:
    return SnapshotStore(snapshot_dir)


def test_fixture_covers_every_snapshot_table() -> None:
    assert set(SNAPSHOT_ROWS) == set(SNAPSHOT_TABLES)


def test_months(store: SnapshotStore) -> None:
    assert store.months() == [FEB, MAR]


def test_driver_economics_for_one_month_ordered_by_trips(store: SnapshotStore) -> None:
    rows = store.driver_economics(MAR)

    assert [(r["company"], r["trips"]) for r in rows] == [("Uber", 320), ("Lyft", 120)]
    assert rows[0]["driver_pay_per_mile"] == 4.0
    assert [r["company"] for r in store.driver_economics(MAR, company="Lyft")] == ["Lyft"]
    assert store.driver_economics(MAR, company="Juno") == []


def test_daily_trips_between_two_dates(store: SnapshotStore) -> None:
    rows = store.daily_trips(FEB.replace(day=28), MAR, company=None)

    assert [(r["trip_date"].isoformat(), r["company"]) for r in rows] == [
        ("2025-02-28", "Lyft"),
        ("2025-02-28", "Uber"),
        ("2025-03-01", "Lyft"),
        ("2025-03-01", "Uber"),
    ]


def test_busiest_zones_add_up_the_days_of_the_month(store: SnapshotStore) -> None:
    rows = store.busiest_zones(MAR)

    assert [(r["zone_id"], r["trips"]) for r in rows] == [(161, 130), (132, 90), (264, 5)]
    assert rows[2]["borough"] is None


def test_busiest_zones_at_one_hour_and_limit(store: SnapshotStore) -> None:
    assert [(r["zone_id"], r["trips"]) for r in store.busiest_zones(MAR, hour=18)] == [(132, 90), (161, 60)]
    assert [r["zone_id"] for r in store.busiest_zones(MAR, limit=1)] == [161]


def test_quality_flags_spikes_like_the_dashboard(store: SnapshotStore) -> None:
    feb, mar = store.quality()

    assert (feb["data_month"], feb["rows_checked"], feb["rows_quarantined"]) == (FEB, 10_000, 3)
    assert feb["spike_rules"] == []  # no previous month
    assert mar["spike_rules"] == ["W003"]  # Q004 doubles too, but with fewer than 100 rows
    assert mar["quarantine_rate"] == pytest.approx(0.0007)


def test_spike_definition_matches_the_dashboard_query() -> None:
    dashboard = (REPO / "dashboards" / "queries" / "quality_rule_rates.sql").read_text()

    assert re.search(rf"matched_rows >= {SPIKE_MIN_ROWS}\b", dashboard)
    assert re.search(rf"match_rate >= {SPIKE_FACTOR} \* previous_match_rate", dashboard)


def test_a_missing_table_stops_the_api_from_starting(tmp_path: Path) -> None:
    partial = {name: SNAPSHOT_ROWS[name] for name in ("monthly_driver_economics",)}
    write_snapshot(tmp_path / "partial", partial)

    with pytest.raises(SnapshotNotFoundError, match="daily_company_trips"):
        SnapshotStore(tmp_path / "partial")
