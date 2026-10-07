"""Silver applies every ADR 0006 rule correctly and is idempotent."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from factories import bronze_frame, read_delta, trip_row, zones_frame  # noqa: E402

from mobility_lakehouse.delta_io import month_predicate  # noqa: E402
from mobility_lakehouse.silver import (  # noqa: E402
    SilverTargets,
    build_silver,
    evaluate,
    quarantine_rows,
    silver_rows,
)
from mobility_lakehouse.tlc import Month  # noqa: E402

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

pytestmark = pytest.mark.spark

JAN = Month.parse("2025-01")
FEB = Month.parse("2025-02")


def evaluated_one(spark: SparkSession, **overrides: Any) -> DataFrame:
    """Evaluate a single January trip with some fields changed."""
    return evaluate(bronze_frame(spark, JAN, [trip_row(JAN, 1, **overrides)]), zones_frame(spark))


def first_row(df: DataFrame) -> Any:
    row = df.first()
    assert row is not None
    return row


def targets(base: Path) -> SilverTargets:
    return SilverTargets(
        silver=str(base / "silver"),
        quarantine=str(base / "quarantine"),
        rule_counts=str(base / "rule_counts"),
    )


def test_a_valid_trip_goes_to_silver_with_zone_names(spark: SparkSession) -> None:
    evaluated = evaluated_one(spark, PULocationID=1, DOLocationID=2)

    assert quarantine_rows(evaluated).count() == 0
    row = first_row(silver_rows(evaluated))
    assert row["quality_warnings"] == []
    assert (row["pickup_borough"], row["pickup_zone"]) == ("Manhattan", "Zone 1")
    assert (row["dropoff_borough"], row["dropoff_zone"]) == ("Queens", "Zone 2")


@pytest.mark.parametrize(
    ("rule_id", "overrides"),
    [
        ("Q001", {"driver_pay": None}),
        ("Q004", {"base_passenger_fare": -5.0}),
        ("Q005", {"driver_pay": -1.0}),
        ("Q006", {"trip_miles": -0.1}),
        ("Q007", {"DOLocationID": 300}),
    ],
)
def test_each_reject_rule_sends_the_row_to_quarantine(
    spark: SparkSession, rule_id: str, overrides: dict[str, Any]
) -> None:
    evaluated = evaluated_one(spark, **overrides)

    assert silver_rows(evaluated).count() == 0
    assert first_row(quarantine_rows(evaluated))["failed_rules"] == [rule_id]


def test_missing_times_are_reported_once_as_required_fields(spark: SparkSession) -> None:
    evaluated = evaluated_one(spark, pickup_datetime=None, dropoff_datetime=None)

    assert first_row(quarantine_rows(evaluated))["failed_rules"] == ["Q001"]


def test_drop_off_before_pickup_is_rejected(spark: SparkSession) -> None:
    pickup = trip_row(JAN, 1)["pickup_datetime"]
    evaluated = evaluated_one(spark, dropoff_datetime=pickup - timedelta(minutes=1))

    assert first_row(quarantine_rows(evaluated))["failed_rules"] == ["Q003"]


def test_pickup_outside_the_partition_month_is_rejected(spark: SparkSession) -> None:
    february_pickup = trip_row(FEB, 1)["pickup_datetime"]
    evaluated = evaluated_one(
        spark,
        pickup_datetime=february_pickup,
        request_datetime=february_pickup - timedelta(minutes=5),
        dropoff_datetime=february_pickup + timedelta(minutes=20),
    )

    assert first_row(quarantine_rows(evaluated))["failed_rules"] == ["Q002"]


def test_a_row_can_fail_several_rules(spark: SparkSession) -> None:
    evaluated = evaluated_one(spark, base_passenger_fare=-5.0, driver_pay=-1.0)

    assert first_row(quarantine_rows(evaluated))["failed_rules"] == ["Q004", "Q005"]


@pytest.mark.parametrize(
    ("rule_id", "overrides"),
    [
        ("W002", {"trip_miles": 0.0}),
        ("W003", {"base_passenger_fare": 0.0}),
        ("W004", {"trip_time": 6 * 60 * 60 + 1}),
    ],
)
def test_warnings_keep_the_row_in_silver(
    spark: SparkSession, rule_id: str, overrides: dict[str, Any]
) -> None:
    evaluated = evaluated_one(spark, **overrides)

    assert quarantine_rows(evaluated).count() == 0
    assert first_row(silver_rows(evaluated))["quality_warnings"] == [rule_id]


def test_pickup_before_request_is_a_warning(spark: SparkSession) -> None:
    pickup = trip_row(JAN, 1)["pickup_datetime"]
    evaluated = evaluated_one(spark, request_datetime=pickup + timedelta(minutes=1))

    assert first_row(silver_rows(evaluated))["quality_warnings"] == ["W001"]


def test_candidate_key_collisions_are_warnings_on_both_rows(spark: SparkSession) -> None:
    twin = trip_row(JAN, 1)
    other = {**twin, "trip_miles": 9.9}  # same key columns, different trip
    evaluated = evaluate(bronze_frame(spark, JAN, [twin, other]), zones_frame(spark))

    silver = silver_rows(evaluated)
    assert silver.count() == 2
    assert silver.where("array_contains(quality_warnings, 'W005')").count() == 2


def test_unknown_zone_keeps_the_trip_with_null_names(spark: SparkSession) -> None:
    evaluated = evaluated_one(spark, PULocationID=265)

    row = first_row(silver_rows(evaluated))
    assert row["pickup_zone"] is None


def test_build_silver_writes_three_tables_and_counts_rules(spark: SparkSession, tmp_path: Path) -> None:
    rows = [trip_row(JAN, n) for n in range(8)]
    rows.append(trip_row(JAN, 8, base_passenger_fare=-5.0))
    rows.append(trip_row(JAN, 9, trip_miles=0.0))
    t = targets(tmp_path)

    result = build_silver(bronze_frame(spark, JAN, rows), zones_frame(spark), JAN, t)

    assert (result.total_rows, result.silver_rows, result.quarantined_rows) == (10, 9, 1)
    counts = {c.rule_id: c.matched_rows for c in result.rule_counts}
    assert counts["Q004"] == 1
    assert counts["W002"] == 1
    assert sum(counts.values()) == 2
    assert read_delta(spark, Path(t.silver)).count() == 9
    assert read_delta(spark, Path(t.quarantine)).count() == 1
    assert read_delta(spark, Path(t.rule_counts)).count() == 12


def test_rerunning_a_month_gives_the_same_tables(spark: SparkSession, tmp_path: Path) -> None:
    rows = [trip_row(JAN, n) for n in range(5)] + [trip_row(JAN, 5, driver_pay=-1.0)]
    t = targets(tmp_path)

    build_silver(bronze_frame(spark, JAN, rows), zones_frame(spark), JAN, t)
    first = {name: read_delta(spark, Path(path)).cache() for name, path in vars(t).items()}
    for frame in first.values():
        frame.count()

    build_silver(bronze_frame(spark, JAN, rows), zones_frame(spark), JAN, t)

    for name, path in vars(t).items():
        second = read_delta(spark, Path(path))
        assert first[name].exceptAll(second).isEmpty(), name
        assert second.exceptAll(first[name]).isEmpty(), name


def test_rerunning_one_month_does_not_touch_another(spark: SparkSession, tmp_path: Path) -> None:
    t = targets(tmp_path)
    build_silver(bronze_frame(spark, JAN, [trip_row(JAN, n) for n in range(4)]), zones_frame(spark), JAN, t)
    build_silver(bronze_frame(spark, FEB, [trip_row(FEB, n) for n in range(3)]), zones_frame(spark), FEB, t)

    build_silver(bronze_frame(spark, JAN, [trip_row(JAN, n) for n in range(2)]), zones_frame(spark), JAN, t)

    silver = read_delta(spark, Path(t.silver))
    assert silver.where(month_predicate(JAN)).count() == 2
    assert silver.where(month_predicate(FEB)).count() == 3
