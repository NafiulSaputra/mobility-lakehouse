"""Gold layer: analysis-ready tables built from one silver month (ADR 0008).

Each table answers one business question and replaces only the month being built:

- daily_company_trips: trips, fares and driver pay per day per company
- hourly_pickup_zones: pickups per zone per hour, for busy-zone analysis
- monthly_driver_economics: what drivers earn per mile and per minute, per company

Ratios are computed from totals (sum of pay / sum of miles), never as an average of per-trip ratios, so long
and short trips are weighted by their actual size.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from mobility_lakehouse.delta_io import PARTITION_COLUMN, Table, write_month
from mobility_lakehouse.tlc import Month

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

# HVFHS licensees from the TLC data dictionary ("as of September 2019"). Unknown codes are kept, not dropped.
COMPANIES = {
    "HV0002": "Juno",
    "HV0003": "Uber",
    "HV0004": "Via",
    "HV0005": "Lyft",
}

# Everything the passenger paid. The congestion fee is null before 2025 (ADR 0006):
# no fee existed then, so it adds 0.
PASSENGER_CHARGES = (
    "base_passenger_fare",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
    "tips",
)


def company_name(code: str | None) -> str:
    """Plain-Python version of the company mapping, used in tests and documentation."""
    if code is None:
        return "Unknown"
    return COMPANIES.get(code, f"Other ({code})")


def _company() -> Column:
    from pyspark.sql import functions as F

    code = F.col("hvfhs_license_num")
    mapped = F.lit(None).cast("string")
    for license_code, name in COMPANIES.items():
        mapped = F.when(code == license_code, F.lit(name)).otherwise(mapped)
    return F.coalesce(mapped, F.concat(F.lit("Other ("), code, F.lit(")")), F.lit("Unknown"))


def _money(column: str) -> Column:
    """Sum of a money column, rounded to cents."""
    from pyspark.sql import functions as F

    return F.round(F.sum(F.coalesce(F.col(column), F.lit(0.0))), 2)


def _ratio(numerator: Column, denominator: Column, digits: int = 4) -> Column:
    """numerator / denominator, or null when the denominator is zero."""
    from pyspark.sql import functions as F

    return F.when(denominator > 0, F.round(numerator / denominator, digits))


def daily_company_trips(silver: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    passenger_total = sum(F.coalesce(F.col(c), F.lit(0.0)) for c in PASSENGER_CHARGES)
    return (
        silver.withColumn("company", _company())
        .withColumn("trip_date", F.to_date("pickup_datetime"))
        .withColumn("_passenger_total", passenger_total)
        .groupBy(PARTITION_COLUMN, "trip_date", "company", "hvfhs_license_num")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            _money("base_passenger_fare").alias("base_fare_total"),
            _money("tips").alias("tips_total"),
            _money("driver_pay").alias("driver_pay_total"),
            _money("_passenger_total").alias("passenger_paid_total"),
            F.round(F.avg("trip_miles"), 3).alias("avg_trip_miles"),
            F.round(F.avg("trip_time") / 60, 2).alias("avg_trip_minutes"),
        )
    )


def hourly_pickup_zones(silver: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return (
        silver.withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .groupBy(
            PARTITION_COLUMN,
            "pickup_date",
            "pickup_hour",
            "PULocationID",
            "pickup_borough",
            "pickup_zone",
        )
        .agg(F.count(F.lit(1)).alias("trips"))
    )


def monthly_driver_economics(silver: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    pay = F.sum("driver_pay")
    miles = F.sum("trip_miles")
    minutes = F.sum("trip_time") / 60
    fare = F.sum("base_passenger_fare")
    return (
        silver.withColumn("company", _company())
        .groupBy(PARTITION_COLUMN, "company", "hvfhs_license_num")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(pay, 2).alias("driver_pay_total"),
            F.round(miles, 2).alias("trip_miles_total"),
            F.round(minutes, 2).alias("trip_minutes_total"),
            _ratio(pay, miles).alias("driver_pay_per_mile"),
            _ratio(pay, minutes).alias("driver_pay_per_minute"),
            _ratio(pay, fare).alias("driver_share_of_base_fare"),
        )
    )


@dataclass(frozen=True)
class GoldTargets:
    """The gold tables."""

    daily_company_trips: Table
    hourly_pickup_zones: Table
    monthly_driver_economics: Table


def build_gold(silver_month: DataFrame, month: Month, targets: GoldTargets) -> None:
    """Rebuild every gold table for one month from that month's silver rows."""
    write_month(daily_company_trips(silver_month), month, targets.daily_company_trips)
    write_month(hourly_pickup_zones(silver_month), month, targets.hourly_pickup_zones)
    write_month(monthly_driver_economics(silver_month), month, targets.monthly_driver_economics)
