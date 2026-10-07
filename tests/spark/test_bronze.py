"""Bronze must be idempotent: the same month loaded twice gives the same table (ADR 0003, 0007)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from factories import FIXED_TIME, load_bronze, raw_month, read_delta  # noqa: E402

from mobility_lakehouse.bronze import month_predicate, prepare_bronze  # noqa: E402
from mobility_lakehouse.contracts import HVFHV_SOURCE, SchemaContractError  # noqa: E402
from mobility_lakehouse.tlc import Month  # noqa: E402

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

pytestmark = pytest.mark.spark

JAN = Month.parse("2025-01")
FEB = Month.parse("2025-02")
JUN_2024 = Month.parse("2024-06")


def load(spark: SparkSession, table: Path, month: Month, rows: int, **kwargs) -> None:
    load_bronze(spark, table, month, rows, **kwargs)


def read(spark: SparkSession, table: Path) -> DataFrame:
    return read_delta(spark, table)


def month_rows(spark: SparkSession, table: Path, month: Month, condition: str = "true") -> int:
    return read(spark, table).where(f"{month_predicate(month)} AND ({condition})").count()


def test_loading_the_same_month_twice_gives_the_same_table(spark: SparkSession, tmp_path: Path) -> None:
    table = tmp_path / "bronze"
    load(spark, table, JAN, rows=50)
    first = read(spark, table).cache()
    first.count()

    load(spark, table, JAN, rows=50)
    second = read(spark, table)

    assert second.count() == 50
    assert first.exceptAll(second).isEmpty()
    assert second.exceptAll(first).isEmpty()


def test_reloading_a_month_does_not_touch_other_months(spark: SparkSession, tmp_path: Path) -> None:
    table = tmp_path / "bronze"
    load(spark, table, JAN, rows=30)
    load(spark, table, FEB, rows=20)

    # TLC republishes January with corrected fares and fewer rows.
    load(spark, table, JAN, rows=25, fare=21.0)

    assert month_rows(spark, table, JAN) == 25
    assert month_rows(spark, table, JAN, "base_passenger_fare <> 21.0") == 0
    assert month_rows(spark, table, FEB) == 20


def test_months_with_and_without_the_congestion_fee_share_one_table(
    spark: SparkSession, tmp_path: Path
) -> None:
    table = tmp_path / "bronze"
    load(spark, table, JUN_2024, rows=10, with_cbd=False)
    load(spark, table, JAN, rows=10)

    assert read(spark, table).count() == 20
    assert month_rows(spark, table, JUN_2024, "cbd_congestion_fee IS NULL") == 10
    assert month_rows(spark, table, JAN, "cbd_congestion_fee = 1.5") == 10


def test_bronze_follows_the_contract_and_adds_metadata(spark: SparkSession) -> None:
    bronze = prepare_bronze(raw_month(spark, JAN, 3), JAN, source_file="f.parquet", ingested_at=FIXED_TIME)

    expected = [c.name for c in HVFHV_SOURCE] + ["_source_file", "_ingested_at", "data_month"]
    assert bronze.columns == expected
    first = bronze.first()
    assert first["_source_file"] == "f.parquet"
    assert first["data_month"] == date(2025, 1, 1)
    assert dict(bronze.dtypes)["pickup_datetime"] == "timestamp_ntz"


def test_unknown_source_column_stops_the_load_and_leaves_the_table_unchanged(
    spark: SparkSession, tmp_path: Path
) -> None:
    from pyspark.sql import functions as F

    table = tmp_path / "bronze"
    load(spark, table, JAN, rows=5)
    changed = raw_month(spark, FEB, 5).withColumn("surprise_fee", F.lit(1.0))

    with pytest.raises(SchemaContractError, match="surprise_fee"):
        prepare_bronze(changed, FEB, source_file="f.parquet")

    assert read(spark, table).count() == 5
