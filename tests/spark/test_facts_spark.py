"""The trip fact table: grain, keys, measures and idempotent writes (ADR 0014)."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from factories import bronze_frame, trip_row, zones_frame  # noqa: E402

from mobility_lakehouse.delta_io import Table, month_predicate  # noqa: E402
from mobility_lakehouse.dimensions import DimensionTargets, build_dimensions  # noqa: E402
from mobility_lakehouse.facts import FCT_TRIPS_COLUMNS, build_fct_trips, fct_trips  # noqa: E402
from mobility_lakehouse.gold import daily_company_trips  # noqa: E402
from mobility_lakehouse.silver import evaluate, silver_rows  # noqa: E402
from mobility_lakehouse.tlc import Month  # noqa: E402

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession

pytestmark = pytest.mark.spark

JAN = Month.parse("2025-01")
FEB = Month.parse("2025-02")
JUN_2024 = Month.parse("2024-06")
OUTSIDE_CALENDAR = Month.parse("2023-12")


def rows(df: DataFrame) -> list[Row]:
    """Collect a small test result. The limit keeps the test bounded, like every Spark test in this repo."""
    return df.limit(10_000).collect()


def silver_frame(spark: SparkSession, month: Month, trips: list[dict]) -> DataFrame:
    return silver_rows(evaluate(bronze_frame(spark, month, trips), zones_frame(spark)))


def trip(month: Month, n: int, **overrides: Any) -> dict:
    return trip_row(month, n, **overrides)


def only_row(df: DataFrame) -> Row:
    found = df.limit(2).collect()
    assert len(found) == 1, found
    return found[0]


def test_columns_are_keys_then_measures(spark: SparkSession) -> None:
    facts = fct_trips(silver_frame(spark, JAN, [trip(JAN, 0)]))

    assert tuple(facts.columns) == FCT_TRIPS_COLUMNS


def test_one_row_per_silver_trip(spark: SparkSession) -> None:
    silver = silver_frame(spark, JAN, [trip(JAN, n) for n in range(30)])

    assert fct_trips(silver).count() == silver.count() == 30


def test_quarantined_trips_are_not_facts(spark: SparkSession) -> None:
    silver = silver_frame(spark, JAN, [trip(JAN, 0), trip(JAN, 1, base_passenger_fare=-5.0)])

    assert fct_trips(silver).count() == 1


def test_identical_trips_get_different_keys(spark: SparkSession) -> None:
    """The TLC data has no trip ID and identical rows exist (rule W005): both must be counted once each."""
    silver = silver_frame(spark, JAN, [trip(JAN, 0), trip(JAN, 0), trip(JAN, 0), trip(JAN, 1)])
    facts = fct_trips(silver)

    assert facts.count() == 4
    assert facts.select("trip_key").distinct().count() == 4


def test_trip_keys_are_the_same_on_every_run(spark: SparkSession) -> None:
    trips = [trip(JAN, 0), trip(JAN, 0), trip(JAN, 5), trip(JAN, 9, hvfhs_license_num="HV0005")]

    def keys() -> list[int]:
        return sorted(r["trip_key"] for r in rows(fct_trips(silver_frame(spark, JAN, trips))))

    assert keys() == keys()


def test_keys_come_from_pickup_time_zones_and_licence(spark: SparkSession) -> None:
    pickup = datetime(2025, 1, 18, 23, 45)
    silver = silver_frame(
        spark,
        JAN,
        [
            trip(
                JAN,
                0,
                pickup_datetime=pickup,
                request_datetime=pickup - timedelta(minutes=4),
                dropoff_datetime=pickup + timedelta(minutes=30),  # drop-off on the next day
                PULocationID=132,
                DOLocationID=161,
            )
        ],
    )

    fact = only_row(fct_trips(silver))

    assert (fact["date_key"], fact["time_key"]) == (20250118, 23)
    assert (fact["pickup_zone_key"], fact["dropoff_zone_key"]) == (132, 161)


def test_total_fare_is_every_charge_the_passenger_paid(spark: SparkSession) -> None:
    # 20 fare + 0.5 bcf + 1.6 tax + 2.75 congestion + 1.5 CBD fee + 3 tip + 6.94 tolls
    silver = silver_frame(spark, JAN, [trip(JAN, 0, tips=3.0, tolls=6.94)])

    assert only_row(fct_trips(silver))["total_fare"] == 36.29


def test_missing_cbd_fee_before_2025_adds_nothing(spark: SparkSession) -> None:
    silver = silver_frame(spark, JUN_2024, [trip(JUN_2024, 0, cbd_congestion_fee=None)])
    fact = only_row(fct_trips(silver))

    assert fact["cbd_congestion_fee"] is None
    assert fact["total_fare"] == 24.85


@pytest.mark.parametrize(
    ("request_offset_seconds", "expected"),
    [(300, 300), (0, 0), (-120, None)],
)
def test_wait_time_is_null_when_pickup_is_before_request(
    spark: SparkSession, request_offset_seconds: int, expected: int | None
) -> None:
    pickup = datetime(2025, 1, 10, 8, 0)
    request = pickup - timedelta(seconds=request_offset_seconds)
    moved = trip(
        JAN,
        0,
        pickup_datetime=pickup,
        dropoff_datetime=pickup + timedelta(minutes=20),  # keep drop-off after pickup (rule Q003)
        request_datetime=request,
    )
    silver = silver_frame(spark, JAN, [moved])

    assert only_row(fct_trips(silver))["wait_time_seconds"] == expected


def test_missing_request_time_gives_null_wait(spark: SparkSession) -> None:
    silver = silver_frame(spark, JAN, [trip(JAN, 0, request_datetime=None)])

    assert only_row(fct_trips(silver))["wait_time_seconds"] is None


def test_every_key_finds_its_dimension_row(spark: SparkSession, tmp_path: Path) -> None:
    """No orphan keys: an inner join with every dimension keeps every fact row."""
    trips = [trip(JAN, n) for n in range(40)] + [trip(JAN, 40, hvfhs_license_num="HV0099")]
    silver = silver_frame(spark, JAN, trips)
    dims = DimensionTargets(
        date=Table(path=str(tmp_path / "dim_date")),
        time=Table(path=str(tmp_path / "dim_time")),
        zone=Table(path=str(tmp_path / "dim_zone")),
        company=Table(path=str(tmp_path / "dim_company")),
    )
    build_dimensions(spark, zones_frame(spark), silver, dims)
    facts = fct_trips(silver)

    lookups = {
        "date_key": (dims.date, "date_key"),
        "time_key": (dims.time, "time_key"),
        "pickup_zone_key": (dims.zone, "zone_key"),
        "dropoff_zone_key": (dims.zone, "zone_key"),
        "company_key": (dims.company, "company_key"),
    }
    for fact_key, (table, dim_key) in lookups.items():
        dimension = table.read(spark).select(dim_key)
        orphans = facts.join(dimension, facts[fact_key] == dimension[dim_key], "left_anti").count()
        assert orphans == 0, f"{fact_key} has {orphans} rows without a {dim_key}"


def test_daily_totals_reconcile_with_gold(spark: SparkSession) -> None:
    """Star and gold describe the same trips: trips and passenger totals per day and company agree."""
    from pyspark.sql import functions as F

    trips = [trip(JAN, n, tips=float(n % 4), tolls=1.25 * (n % 3)) for n in range(60)]
    trips += [trip(JAN, n, hvfhs_license_num="HV0005") for n in range(15)]
    silver = silver_frame(spark, JAN, trips)

    star = (
        fct_trips(silver)
        .groupBy("date_key", "company_key")
        .agg(F.count(F.lit(1)).alias("trips"), F.round(F.sum("total_fare"), 2).alias("paid"))
    )
    gold = daily_company_trips(silver).select(
        F.date_format("trip_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.xxhash64("hvfhs_license_num").alias("company_key"),
        "trips",
        F.col("passenger_paid_total").alias("paid"),
    )

    assert star.exceptAll(gold).count() == 0
    assert gold.exceptAll(star).count() == 0


def test_month_outside_the_calendar_stops_before_writing(spark: SparkSession, tmp_path: Path) -> None:
    target = Table(path=str(tmp_path / "fct_trips"))
    silver = silver_frame(spark, OUTSIDE_CALENDAR, [trip(OUTSIDE_CALENDAR, 0)])

    with pytest.raises(ValueError, match="outside the calendar"):
        build_fct_trips(silver, OUTSIDE_CALENDAR, target)
    assert not (tmp_path / "fct_trips").exists()


def test_rebuilding_a_month_is_idempotent_and_leaves_other_months(
    spark: SparkSession, tmp_path: Path
) -> None:
    target = Table(path=str(tmp_path / "fct_trips"))
    jan = silver_frame(spark, JAN, [trip(JAN, n) for n in range(5)])
    feb = silver_frame(spark, FEB, [trip(FEB, n) for n in range(3)])

    build_fct_trips(jan, JAN, target)
    build_fct_trips(feb, FEB, target)
    first = sorted(r["trip_key"] for r in rows(target.read(spark).where(month_predicate(JAN))))
    build_fct_trips(jan, JAN, target)

    facts = target.read(spark)
    assert sorted(r["trip_key"] for r in rows(facts.where(month_predicate(JAN)))) == first
    assert facts.where(month_predicate(FEB)).count() == 3
    assert facts.count() == 8
