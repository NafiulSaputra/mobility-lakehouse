"""A small snapshot written with DuckDB, laid out like the one Spark writes.

Every table is a folder of Parquet files partitioned by month: <table>/data_month=<date>/*.parquet.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

FEB = date(2025, 2, 1)
MAR = date(2025, 3, 1)

# Columns and rows of every snapshot table, in the order the pipeline writes them.
SNAPSHOT_ROWS: dict[str, tuple[str, list[tuple]]] = {
    "monthly_driver_economics": (
        "data_month, company, hvfhs_license_num, trips, driver_pay_total, trip_miles_total, "
        "trip_minutes_total, driver_pay_per_mile, driver_pay_per_minute, driver_share_of_base_fare",
        [
            (FEB, "Uber", "HV0003", 300, 1200.0, 300.0, 600.0, 4.0, 2.0, 0.75),
            (FEB, "Lyft", "HV0005", 100, 390.0, 100.0, 200.0, 3.9, 1.95, 0.78),
            (MAR, "Uber", "HV0003", 320, 1280.0, 320.0, 640.0, 4.0, 2.0, 0.72),
            (MAR, "Lyft", "HV0005", 120, 480.0, 120.0, 240.0, 4.0, 2.0, 0.79),
        ],
    ),
    "daily_company_trips": (
        "data_month, trip_date, company, hvfhs_license_num, trips, base_fare_total, tips_total, "
        "driver_pay_total, passenger_paid_total, avg_trip_miles, avg_trip_minutes",
        [
            (FEB, date(2025, 2, 28), "Uber", "HV0003", 10, 200.0, 10.0, 150.0, 240.0, 3.5, 20.0),
            (FEB, date(2025, 2, 28), "Lyft", "HV0005", 4, 80.0, 4.0, 60.0, 96.0, 3.4, 19.0),
            (MAR, date(2025, 3, 1), "Uber", "HV0003", 12, 240.0, 12.0, 180.0, 288.0, 3.6, 21.0),
            (MAR, date(2025, 3, 1), "Lyft", "HV0005", 5, 100.0, 5.0, 75.0, 120.0, 3.3, 18.0),
            (MAR, date(2025, 3, 2), "Uber", "HV0003", 11, 220.0, 11.0, 165.0, 264.0, 3.5, 20.0),
        ],
    ),
    "hourly_pickup_zones": (
        "data_month, pickup_date, pickup_hour, PULocationID, pickup_borough, pickup_zone, trips",
        [
            (MAR, date(2025, 3, 1), 18, 132, "Queens", "JFK Airport", 50),
            (MAR, date(2025, 3, 2), 18, 132, "Queens", "JFK Airport", 40),
            (MAR, date(2025, 3, 1), 18, 161, "Manhattan", "Midtown Center", 60),
            (MAR, date(2025, 3, 1), 8, 161, "Manhattan", "Midtown Center", 70),
            (MAR, date(2025, 3, 1), 3, 264, None, None, 5),
            (FEB, date(2025, 2, 28), 18, 132, "Queens", "JFK Airport", 999),
        ],
    ),
    # W003 jumps from 0.02% to 3% of rows with 300 rows: a spike. Q004 more than doubles but has only
    # 7 rows: not a spike. February has no previous month: no spikes.
    "rule_counts": (
        "data_month, rule_id, severity, description, matched_rows, total_rows",
        [
            (FEB, "Q004", "reject", "Negative base fare", 3, 10_000),
            (FEB, "W001", "warning", "Pickup before request", 100, 10_000),
            (FEB, "W003", "warning", "Zero base fare", 2, 10_000),
            (MAR, "Q004", "reject", "Negative base fare", 7, 10_000),
            (MAR, "W001", "warning", "Pickup before request", 110, 10_000),
            (MAR, "W003", "warning", "Zero base fare", 300, 10_000),
        ],
    ),
    "quality_months": (
        "data_month, rows_checked, rows_quarantined",
        [(FEB, 10_000, 3), (MAR, 10_000, 7)],
    ),
}


def write_snapshot(root: Path, tables: dict[str, tuple[str, list[tuple]]] = SNAPSHOT_ROWS) -> Path:
    # DuckDB creates the table folders, but not their parent.
    root.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    for name, (columns, rows) in tables.items():
        width = len(columns.split(","))
        placeholders = ", ".join(["(" + ", ".join(["?"] * width) + ")"] * len(rows))
        values = [value for row in rows for value in row]
        connection.execute(f"CREATE TABLE {name} AS FROM (VALUES {placeholders}) AS t({columns})", values)
        target = (root / name).as_posix()
        connection.execute(f"COPY {name} TO '{target}' (FORMAT parquet, PARTITION_BY (data_month))")
    connection.close()
    return root
