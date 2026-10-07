"""Small, readable test data for Spark tests: trips that follow the HVFHV source contract."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mobility_lakehouse.bronze import prepare_bronze, write_bronze
from mobility_lakehouse.contracts import HVFHV_SOURCE, ddl
from mobility_lakehouse.tlc import Month

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

FIXED_TIME = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
ZONES_DDL = "LocationID string, Borough string, Zone string, service_zone string"


def trip_row(month: Month, n: int, fare: float = 20.0, with_cbd: bool = True, **overrides: Any) -> dict:
    """One valid trip. Pass column=value to change a field, for example driver_pay=-5.0."""
    pickup = datetime(month.year, month.month, 1 + n % 27, 8, n % 60)
    row = {
        "hvfhs_license_num": "HV0003",
        "dispatching_base_num": "B03404",
        "originating_base_num": None,
        "request_datetime": pickup - timedelta(minutes=5),
        "on_scene_datetime": None,
        "pickup_datetime": pickup,
        "dropoff_datetime": pickup + timedelta(minutes=20),
        "PULocationID": 1 + n % 263,
        "DOLocationID": 1 + (n * 7) % 263,
        "trip_miles": 3.5,
        "trip_time": 1200,
        "base_passenger_fare": fare,
        "tolls": 0.0,
        "bcf": 0.5,
        "sales_tax": 1.6,
        "congestion_surcharge": 2.75,
        "airport_fee": 0.0,
        "tips": 0.0,
        "driver_pay": 15.0,
        "shared_request_flag": "N",
        "shared_match_flag": "N",
        "access_a_ride_flag": "N",
        "wav_request_flag": "N",
        "wav_match_flag": "N",
        "cbd_congestion_fee": 1.5,
    }
    row.update(overrides)
    if not with_cbd:
        del row["cbd_congestion_fee"]
    return row


def raw_frame(spark: SparkSession, rows: list[dict], with_cbd: bool = True) -> DataFrame:
    columns = [c for c in HVFHV_SOURCE if with_cbd or c.name != "cbd_congestion_fee"]
    return spark.createDataFrame([tuple(r[c.name] for c in columns) for r in rows], schema=ddl(columns))


def raw_month(
    spark: SparkSession,
    month: Month,
    rows: int,
    fare: float = 20.0,
    with_cbd: bool = True,
) -> DataFrame:
    return raw_frame(spark, [trip_row(month, n, fare, with_cbd) for n in range(rows)], with_cbd)


def bronze_frame(spark: SparkSession, month: Month, rows: list[dict]) -> DataFrame:
    """Bronze rows exactly as the bronze layer would produce them."""
    raw = raw_frame(spark, rows)
    return prepare_bronze(raw, month, source_file=f"test_{month}.parquet", ingested_at=FIXED_TIME)


def load_bronze(spark: SparkSession, table: Path, month: Month, rows: int, **kwargs: Any) -> None:
    raw = raw_month(spark, month, rows, **kwargs)
    bronze = prepare_bronze(raw, month, source_file=f"fhvhv_tripdata_{month}.parquet", ingested_at=FIXED_TIME)
    write_bronze(bronze, month, path=str(table))


def zones_frame(spark: SparkSession) -> DataFrame:
    """Zone lookup shaped like the TLC CSV (read with a header, so every column is a string)."""
    rows = [(str(i), "Manhattan" if i % 2 else "Queens", f"Zone {i}", "Yellow Zone") for i in range(1, 264)]
    rows.append(("264", "Unknown", "N/A", "N/A"))
    return spark.createDataFrame(rows, schema=ZONES_DDL)


def read_delta(spark: SparkSession, table: Path) -> DataFrame:
    return spark.read.format("delta").load(str(table))
