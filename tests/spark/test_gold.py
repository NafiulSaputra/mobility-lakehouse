"""Gold tables answer their business questions correctly and are idempotent (ADR 0008)."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from factories import bronze_frame, trip_row, zones_frame  # noqa: E402

from mobility_lakehouse.delta_io import Table, month_predicate  # noqa: E402
from mobility_lakehouse.gold import (  # noqa: E402
    GoldTargets,
    build_gold,
    daily_company_trips,
    hourly_pickup_zones,
    monthly_driver_economics,
)
from mobility_lakehouse.silver import evaluate, silver_rows  # noqa: E402
from mobility_lakehouse.tlc import Month  # noqa: E402

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

pytestmark = pytest.mark.spark

JAN = Month.parse("2025-01")
FEB = Month.parse("2025-02")
JUN_2024 = Month.parse("2024-06")
UBER = "HV0003"
LYFT = "HV0005"


def silver_frame(spark: SparkSession, month: Month, rows: list[dict]) -> DataFrame:
    return silver_rows(evaluate(bronze_frame(spark, month, rows), zones_frame(spark)))


def trip(month: Month, n: int, **overrides: Any) -> dict:
    return trip_row(month, n, **overrides)


def only_row(df: DataFrame) -> Any:
    rows = df.limit(2).collect()
    assert len(rows) == 1, rows
    return rows[0]


def test_daily_trips_are_counted_per_day_and_company(spark: SparkSession) -> None:
    # trip_row(month, n) puts trip n on day 1 + n, so trips 0 and 27 share a day.
    rows = [trip(JAN, 0), trip(JAN, 27), trip(JAN, 0, hvfhs_license_num=LYFT, PULocationID=9)]
    daily = daily_company_trips(silver_frame(spark, JAN, rows))

    uber = only_row(daily.where(f"company = 'Uber' AND hvfhs_license_num = '{UBER}'"))
    assert uber["trips"] == 2
    assert uber["base_fare_total"] == 40.0
    assert uber["driver_pay_total"] == 30.0
    # 20 fare + 0.5 bcf + 1.6 tax + 2.75 congestion + 1.5 CBD fee per trip
    assert uber["passenger_paid_total"] == 52.7
    assert only_row(daily.where("company = 'Lyft'"))["trips"] == 1


def test_missing_congestion_fee_before_2025_adds_nothing(spark: SparkSession) -> None:
    daily = daily_company_trips(silver_frame(spark, JUN_2024, [trip(JUN_2024, 0, cbd_congestion_fee=None)]))

    assert only_row(daily)["passenger_paid_total"] == 24.85


def test_unknown_licensee_is_kept(spark: SparkSession) -> None:
    daily = daily_company_trips(silver_frame(spark, JAN, [trip(JAN, 0, hvfhs_license_num="HV0099")]))

    assert only_row(daily)["company"] == "Other (HV0099)"


def test_hourly_pickups_are_counted_per_zone_and_hour(spark: SparkSession) -> None:
    rows = [trip(JAN, 0, PULocationID=7), trip(JAN, 27, PULocationID=7), trip(JAN, 1, PULocationID=8)]
    hourly = hourly_pickup_zones(silver_frame(spark, JAN, rows))

    zone_7 = only_row(hourly.where("PULocationID = 7"))
    assert (zone_7["pickup_hour"], zone_7["trips"]) == (8, 2)
    assert (zone_7["pickup_borough"], zone_7["pickup_zone"]) == ("Manhattan", "Zone 7")
    assert hourly.count() == 2


def test_driver_pay_per_mile_is_a_ratio_of_totals(spark: SparkSession) -> None:
    """Total pay / total miles = 40 / 10 = 4.0.

    Averaging the per-trip ratios would give (10 + 3.33) / 2 = 6.67, overweighting the short trip.
    """
    rows = [
        trip(JAN, 0, driver_pay=10.0, trip_miles=1.0, trip_time=600, base_passenger_fare=20.0),
        trip(JAN, 1, driver_pay=30.0, trip_miles=9.0, trip_time=1800, base_passenger_fare=60.0),
    ]
    economics = only_row(monthly_driver_economics(silver_frame(spark, JAN, rows)))

    assert economics["trips"] == 2
    assert economics["driver_pay_per_mile"] == 4.0
    assert economics["driver_pay_per_minute"] == 1.0  # 40 pay / 40 minutes
    assert economics["driver_share_of_base_fare"] == 0.5  # 40 pay / 80 fare


def test_zero_miles_give_null_instead_of_division_by_zero(spark: SparkSession) -> None:
    economics = monthly_driver_economics(silver_frame(spark, JAN, [trip(JAN, 0, trip_miles=0.0)]))

    assert only_row(economics)["driver_pay_per_mile"] is None


def test_rebuilding_a_month_is_idempotent_and_leaves_other_months(
    spark: SparkSession, tmp_path: Path
) -> None:
    targets = GoldTargets(
        daily_company_trips=Table(path=str(tmp_path / "daily")),
        hourly_pickup_zones=Table(path=str(tmp_path / "hourly")),
        monthly_driver_economics=Table(path=str(tmp_path / "economics")),
    )
    jan = silver_frame(spark, JAN, [trip(JAN, n) for n in range(5)])
    build_gold(jan, JAN, targets)
    build_gold(silver_frame(spark, FEB, [trip(FEB, n) for n in range(3)]), FEB, targets)
    first = targets.daily_company_trips.read(spark).cache()
    first.count()

    build_gold(jan, JAN, targets)

    second = targets.daily_company_trips.read(spark)
    assert first.exceptAll(second).isEmpty()
    assert second.exceptAll(first).isEmpty()
    economics = targets.monthly_driver_economics.read(spark)
    assert only_row(economics.where(month_predicate(JAN)))["trips"] == 5
    assert only_row(economics.where(month_predicate(FEB)))["trips"] == 3
