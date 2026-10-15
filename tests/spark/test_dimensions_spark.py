"""Star schema dimensions as Delta tables: keys, contents and idempotent writes (ADR 0014)."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from factories import zones_frame  # noqa: E402

from mobility_lakehouse.delta_io import Table, replace_table, upsert  # noqa: E402
from mobility_lakehouse.dimensions import (  # noqa: E402
    DIM_COMPANY_DDL,
    DimensionTargets,
    build_dimensions,
    date_key,
    date_key_column,
    date_rows,
    dim_company_rows,
    dim_date,
    dim_time,
    dim_zone,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession

pytestmark = pytest.mark.spark

ZONES_DDL = "LocationID string, Borough string, Zone string, service_zone string"


def rows(df: DataFrame) -> list[Row]:
    """Collect a small test result. The limit keeps the test bounded, like every Spark test in this repo."""
    return df.limit(10_000).collect()


def codes_frame(spark: SparkSession, *codes: str) -> DataFrame:
    """The only silver column the company dimension reads."""
    return spark.createDataFrame([(c,) for c in codes], schema="hvfhs_license_num string")


def targets(root: Path) -> DimensionTargets:
    return DimensionTargets(
        date=Table(path=str(root / "dim_date")),
        time=Table(path=str(root / "dim_time")),
        zone=Table(path=str(root / "dim_zone")),
        company=Table(path=str(root / "dim_company")),
    )


def test_dim_date_matches_the_python_calendar(spark: SparkSession) -> None:
    dates = dim_date(spark)

    assert dates.count() == len(date_rows())
    assert dates.select("date_key").distinct().count() == dates.count()
    row = dates.where("date_key = 20250118").first()
    assert (row["date"], row["day_name"], row["is_weekend"]) == (date(2025, 1, 18), "Saturday", True)


def test_date_key_column_matches_date_key(spark: SparkSession) -> None:
    pickups = [datetime(2025, 1, 15, 0, 0), datetime(2025, 1, 15, 23, 59, 59), datetime(2024, 2, 29, 12, 0)]
    frame = spark.createDataFrame([(p,) for p in pickups], schema="pickup_datetime timestamp_ntz")

    keys = [r["k"] for r in rows(frame.select(date_key_column(frame["pickup_datetime"]).alias("k")))]

    assert keys == [date_key(p.date()) for p in pickups]


def test_dim_time_has_one_row_per_hour(spark: SparkSession) -> None:
    hours = dim_time(spark)

    assert sorted(r["time_key"] for r in rows(hours)) == list(range(24))


def test_dim_zone_keys_are_the_tlc_location_ids(spark: SparkSession) -> None:
    zones = dim_zone(zones_frame(spark))

    assert zones.count() == 264
    assert zones.select("zone_key").distinct().count() == 264
    assert dict(zones.dtypes)["zone_key"] == "int"
    unknown = zones.where("zone_key = 264").first()
    assert (unknown["location_id"], unknown["borough"], unknown["is_airport"]) == (264, "Unknown", False)


def test_airport_zones_are_flagged(spark: SparkSession) -> None:
    lookup = spark.createDataFrame(
        [
            ("1", "EWR", "Newark Airport", "EWR"),
            ("132", "Queens", "JFK Airport", "Airports"),
            ("138", "Queens", "LaGuardia Airport", "Airports"),
            ("161", "Manhattan", "Midtown Center", "Yellow Zone"),
            ("265", "N/A", "Outside of NYC", None),
        ],
        schema=ZONES_DDL,
    )

    airports = {r["zone_key"] for r in rows(dim_zone(lookup).where("is_airport"))}

    assert airports == {1, 132, 138}
    assert dim_zone(lookup).where("is_airport IS NULL").count() == 0


def test_known_companies_are_always_present_and_unknown_codes_are_kept(spark: SparkSession) -> None:
    companies = dim_company_rows(codes_frame(spark, "HV0003", "HV0003", "HV0099"))
    names = {r["hvfhs_license_num"]: r["company_name"] for r in rows(companies)}

    assert names == {
        "HV0002": "Juno",
        "HV0003": "Uber",
        "HV0004": "Via",
        "HV0005": "Lyft",
        "HV0099": "Other (HV0099)",
    }
    assert companies.select("company_key").distinct().count() == 5


def test_company_key_is_the_same_on_every_run(spark: SparkSession) -> None:
    def keys() -> dict[str, int]:
        companies = dim_company_rows(codes_frame(spark, "HV0099"))
        return {r["hvfhs_license_num"]: r["company_key"] for r in rows(companies)}

    assert keys() == keys()


def test_replace_table_replaces_every_row(spark: SparkSession, tmp_path: Path) -> None:
    table = Table(path=str(tmp_path / "small"))
    replace_table(spark.createDataFrame([(1,), (2,), (3,)], "id int"), table)
    replace_table(spark.createDataFrame([(7,), (8,)], "id int"), table)

    assert sorted(r["id"] for r in rows(table.read(spark))) == [7, 8]


def test_upsert_inserts_new_keys_updates_existing_ones_and_keeps_the_rest(
    spark: SparkSession, tmp_path: Path
) -> None:
    table = Table(path=str(tmp_path / "companies"))
    upsert(
        spark.createDataFrame([(1, "HV0003", "Old name"), (2, "HV0099", "Other (HV0099)")], DIM_COMPANY_DDL),
        table,
        key="hvfhs_license_num",
        ddl=DIM_COMPANY_DDL,
    )
    upsert(
        spark.createDataFrame([(1, "HV0003", "Uber")], DIM_COMPANY_DDL),
        table,
        key="hvfhs_license_num",
        ddl=DIM_COMPANY_DDL,
    )

    names = {r["hvfhs_license_num"]: r["company_name"] for r in rows(table.read(spark))}
    assert names == {"HV0003": "Uber", "HV0099": "Other (HV0099)"}


def test_building_the_dimensions_is_idempotent(spark: SparkSession, tmp_path: Path) -> None:
    tables = targets(tmp_path)
    zones = zones_frame(spark)

    def counts() -> dict[str, int]:
        return {name: table.read(spark).count() for name, table in vars(tables).items()}

    build_dimensions(spark, zones, codes_frame(spark, "HV0003"), tables)
    first = counts()
    build_dimensions(spark, zones, codes_frame(spark, "HV0003"), tables)

    assert counts() == first
    assert first == {"date": len(date_rows()), "time": 24, "zone": 264, "company": 4}


def test_a_company_seen_in_one_month_stays_when_later_months_are_built(
    spark: SparkSession, tmp_path: Path
) -> None:
    tables = targets(tmp_path)
    zones = zones_frame(spark)

    build_dimensions(spark, zones, codes_frame(spark, "HV0003", "HV0099"), tables)  # e.g. January
    build_dimensions(spark, zones, codes_frame(spark, "HV0005"), tables)  # e.g. February, no HV0099

    codes = {r["hvfhs_license_num"] for r in rows(tables.company.read(spark))}
    assert "HV0099" in codes
    assert len(codes) == 5


def test_calendar_dates_are_real_dates(spark: SparkSession) -> None:
    first = dim_date(spark, date(2025, 3, 1), date(2025, 3, 1)).first()

    assert first["date"] == date(2025, 3, 1)
    assert first["month_start"] == date(2025, 3, 1)
