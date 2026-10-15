"""The hourly fact table: grain, measures and exact reconciliation with fct_trips (ADR 0014)."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from factories import bronze_frame, trip_row, zones_frame  # noqa: E402

from mobility_lakehouse.delta_io import Table, month_predicate  # noqa: E402
from mobility_lakehouse.facts import (  # noqa: E402
    FCT_TRIPS_HOURLY_COLUMNS,
    HOURLY_KEYS,
    MONEY_MEASURES,
    build_fct_trips_hourly,
    fct_trips,
    fct_trips_hourly,
)
from mobility_lakehouse.silver import evaluate, silver_rows  # noqa: E402
from mobility_lakehouse.tlc import Month  # noqa: E402

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession

pytestmark = pytest.mark.spark

JAN = Month.parse("2025-01")
FEB = Month.parse("2025-02")
JUN_2024 = Month.parse("2024-06")


def trip_at(month: Month, n: int, day: int, hour: int, minute: int = 0, **overrides: Any) -> dict:
    """A valid trip picked up at a given day and hour, with drop-off after pickup and a 5-minute wait.

    The pickup zone still comes from ``n`` unless ``PULocationID`` is given, so trips meant for the same
    hourly row must pass the same zone.
    """
    pickup = datetime(month.year, month.month, day, hour, minute)
    values = {
        "pickup_datetime": pickup,
        "request_datetime": pickup - timedelta(minutes=5),
        "dropoff_datetime": pickup + timedelta(minutes=20),
    }
    values.update(overrides)
    return trip_row(month, n, **values)


def facts_of(spark: SparkSession, month: Month, trips: list[dict]) -> DataFrame:
    return fct_trips(silver_rows(evaluate(bronze_frame(spark, month, trips), zones_frame(spark))))


def rows(df: DataFrame) -> list[Row]:
    """Collect a small test result. The limit keeps the test bounded, like every Spark test in this repo."""
    return df.limit(10_000).collect()


def only_row(df: DataFrame) -> Row:
    found = df.limit(2).collect()
    assert len(found) == 1, found
    return found[0]


def mixed_month(month: Month) -> list[dict]:
    """Trips spread over days, hours, zones and companies, with varied money values."""
    trips = []
    for n in range(48):
        trips.append(
            trip_at(
                month,
                n,
                day=1 + n % 5,
                hour=(n * 7) % 24,
                minute=n % 60,
                PULocationID=1 + n % 4,
                hvfhs_license_num="HV0003" if n % 3 else "HV0005",
                tips=float(n % 4),
                tolls=1.25 * (n % 3),
                airport_fee=2.5 if n % 6 == 0 else 0.0,
                trip_miles=0.5 + n / 10,
            )
        )
    return trips


def test_columns_are_keys_then_measures(spark: SparkSession) -> None:
    hourly = fct_trips_hourly(facts_of(spark, JAN, [trip_at(JAN, 0, day=2, hour=8)]))

    assert tuple(hourly.columns) == FCT_TRIPS_HOURLY_COLUMNS


def test_one_row_per_date_hour_zone_and_company(spark: SparkSession) -> None:
    zone = 7
    trips = [
        trip_at(JAN, 0, day=2, hour=8, minute=5, PULocationID=zone),
        trip_at(JAN, 1, day=2, hour=8, minute=40, PULocationID=zone),  # same group as the first trip
        trip_at(JAN, 2, day=2, hour=9, PULocationID=zone),  # other hour
        trip_at(JAN, 3, day=3, hour=8, PULocationID=zone),  # other day
        trip_at(JAN, 4, day=2, hour=8, PULocationID=50),  # other zone
        trip_at(JAN, 5, day=2, hour=8, PULocationID=zone, hvfhs_license_num="HV0005"),  # other company
    ]
    hourly = fct_trips_hourly(facts_of(spark, JAN, trips))

    assert hourly.count() == 5
    assert hourly.select(*HOURLY_KEYS).distinct().count() == 5
    assert sorted(r["trips"] for r in rows(hourly)) == [1, 1, 1, 1, 2]


def test_totals_reconcile_exactly_with_fct_trips(spark: SparkSession) -> None:
    """Every measure, summed over the whole month, is the same in both fact tables."""
    from pyspark.sql import functions as F

    facts = facts_of(spark, JAN, mixed_month(JAN))
    hourly = fct_trips_hourly(facts)

    trip_level = facts.agg(
        F.count(F.lit(1)).alias("trips"),
        F.sum("trip_time").alias("trip_time"),
        F.sum("wait_time_seconds").alias("wait_time_seconds"),
        F.count("wait_time_seconds").alias("wait_time_trips"),
        F.round(F.sum("trip_miles"), 2).alias("trip_miles"),
        *(F.round(F.sum(m), 2).alias(m) for m in MONEY_MEASURES),
    ).first()
    summary = hourly.agg(
        F.sum("trips").alias("trips"),
        F.sum("trip_time_sum").alias("trip_time"),
        F.sum("wait_time_seconds_sum").alias("wait_time_seconds"),
        F.sum("wait_time_trips").alias("wait_time_trips"),
        F.round(F.sum("trip_miles_sum"), 2).alias("trip_miles"),
        *(F.round(F.sum(f"{m}_sum"), 2).alias(m) for m in MONEY_MEASURES),
    ).first()

    assert trip_level["trips"] == 48
    assert summary.asDict() == trip_level.asDict()


def test_each_group_reconciles_with_its_trips(spark: SparkSession) -> None:
    """Trip count and total fare per group match a direct aggregation of fct_trips."""
    from pyspark.sql import functions as F

    facts = facts_of(spark, JAN, mixed_month(JAN))
    expected = facts.groupBy(*HOURLY_KEYS).agg(
        F.count(F.lit(1)).alias("trips"), F.round(F.sum("total_fare"), 2).alias("total_fare_sum")
    )
    actual = fct_trips_hourly(facts).select(*HOURLY_KEYS, "trips", "total_fare_sum")

    assert actual.exceptAll(expected).count() == 0
    assert expected.exceptAll(actual).count() == 0


def test_average_wait_uses_only_trips_with_a_wait(spark: SparkSession) -> None:
    late_request = datetime(2025, 1, 2, 8, 30)
    same_group = {"day": 2, "hour": 8, "PULocationID": 7}
    trips = [
        trip_at(JAN, 0, **same_group),  # 300-second wait
        trip_at(JAN, 1, minute=10, request_datetime=late_request, **same_group),  # negative: no wait
        trip_at(JAN, 2, minute=20, request_datetime=None, **same_group),  # unknown: no wait
    ]
    row = only_row(fct_trips_hourly(facts_of(spark, JAN, trips)))

    assert (row["trips"], row["wait_time_trips"], row["wait_time_seconds_sum"]) == (3, 1, 300)


def test_cbd_fee_stays_null_before_2025(spark: SparkSession) -> None:
    same_group = {"day": 3, "hour": 8, "PULocationID": 7}
    trips = [trip_at(JUN_2024, n, minute=n, cbd_congestion_fee=None, **same_group) for n in range(3)]
    row = only_row(fct_trips_hourly(facts_of(spark, JUN_2024, trips)))

    assert row["cbd_congestion_fee_sum"] is None
    assert row["total_fare_sum"] == round(3 * 24.85, 2)


def test_rebuilding_a_month_is_idempotent_and_leaves_other_months(
    spark: SparkSession, tmp_path: Path
) -> None:
    target = Table(path=str(tmp_path / "fct_trips_hourly"))
    jan = facts_of(spark, JAN, mixed_month(JAN))
    feb = facts_of(spark, FEB, [trip_at(FEB, n, day=1, hour=n) for n in range(3)])

    def january() -> list[tuple]:
        return sorted(tuple(r) for r in rows(target.read(spark).where(month_predicate(JAN))))

    build_fct_trips_hourly(jan, JAN, target)
    build_fct_trips_hourly(feb, FEB, target)
    first = january()  # materialized now, before the rebuild
    build_fct_trips_hourly(jan, JAN, target)

    assert january() == first
    assert target.read(spark).where(month_predicate(FEB)).count() == 3
