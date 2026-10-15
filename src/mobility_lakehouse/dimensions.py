"""Star schema dimensions: calendar, hour of day, taxi zones and companies (ADR 0014).

A dimension describes the context of a trip (when, where, which company) so facts can be filtered and grouped.
Every dimension has a key that is computed from its own values, never a running number, so a rerun produces
exactly the same keys and the facts keep pointing at the right rows:

- ``dim_date``: one row per calendar day, ``date_key`` = yyyymmdd (``20250115``)
- ``dim_time``: one row per hour of the day, ``time_key`` = hour (0-23)
- ``dim_zone``: one row per TLC taxi zone, ``zone_key`` = TLC ``LocationID``
- ``dim_company``: one row per HVFHS licence code, ``company_key`` = hash of the code

Calendar and hour rows are plain Python, so they can be tested without Spark.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING

from mobility_lakehouse.delta_io import Table, replace_table, upsert
from mobility_lakehouse.gold import COMPANIES, company_column
from mobility_lakehouse.tlc import Month

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

# The project scope starts in January 2024 (ADR 0002). The end leaves room for new months; a month outside
# the calendar stops the run instead of producing facts without a date (see ``check_in_calendar``).
CALENDAR_START = date(2024, 1, 1)
CALENDAR_END = date(2027, 12, 31)

# Weekday commuting hours, 07:00-09:59 and 16:00-19:59 (ADR 0014). The flag describes the hour only:
# combine it with dim_date.is_weekend, because a peak hour on a Sunday is not a rush hour.
PEAK_HOURS = frozenset({7, 8, 9, 16, 17, 18, 19})

# First hour of each part of the day.
DAY_PARTS = ((0, "Night"), (6, "Morning"), (12, "Afternoon"), (18, "Evening"))

# service_zone values of the airport zones in the TLC taxi zone lookup: JFK and LaGuardia, and Newark (EWR).
AIRPORT_SERVICE_ZONES = ("Airports", "EWR")

MONTH_NAMES = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)
DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")

DIM_DATE_DDL = (
    "date_key int, date date, year int, quarter int, month int, month_name string, month_start date, "
    "day_of_month int, day_of_week int, day_name string, is_weekend boolean"
)
DIM_TIME_DDL = "time_key int, hour int, hour_label string, day_part string, is_peak_hour boolean"
DIM_COMPANY_DDL = "company_key bigint, hvfhs_license_num string, company_name string"


# ---------------------------------------------------------------------------------------------
# Plain Python: the calendar and the hours of the day.
# ---------------------------------------------------------------------------------------------


def date_key(day: date) -> int:
    """The calendar key of a day: 2025-01-15 becomes 20250115."""
    return day.year * 10_000 + day.month * 100 + day.day


def date_rows(start: date = CALENDAR_START, end: date = CALENDAR_END) -> list[tuple]:
    """One row per day from ``start`` to ``end`` (both included), in the column order of ``DIM_DATE_DDL``."""
    if end < start:
        raise ValueError(f"calendar end {end} is before its start {start}")
    rows = []
    day = start
    while day <= end:
        weekday = day.isoweekday()  # 1 = Monday, 7 = Sunday
        rows.append(
            (
                date_key(day),
                day,
                day.year,
                (day.month - 1) // 3 + 1,
                day.month,
                MONTH_NAMES[day.month - 1],
                day.replace(day=1),
                day.day,
                weekday,
                DAY_NAMES[weekday - 1],
                weekday >= 6,
            )
        )
        day += timedelta(days=1)
    return rows


def day_part(hour: int) -> str:
    """Night (0-5), Morning (6-11), Afternoon (12-17) or Evening (18-23)."""
    if not 0 <= hour <= 23:
        raise ValueError(f"hour must be between 0 and 23, got {hour}")
    return next(name for first_hour, name in reversed(DAY_PARTS) if hour >= first_hour)


def time_rows() -> list[tuple]:
    """One row per hour of the day, in the column order of ``DIM_TIME_DDL``."""
    return [(hour, hour, f"{hour:02d}:00", day_part(hour), hour in PEAK_HOURS) for hour in range(24)]


def check_in_calendar(month: Month) -> None:
    """Stop before building facts for a month that ``dim_date`` does not cover.

    Facts of such a month would point at dates that do not exist in the calendar (orphan keys).
    """
    last_day = month.end - timedelta(days=1)
    if month.start < CALENDAR_START or last_day > CALENDAR_END:
        raise ValueError(
            f"month {month} is outside the calendar dimension ({CALENDAR_START} to {CALENDAR_END}); "
            "extend CALENDAR_END in dimensions.py (ADR 0014)"
        )


# ---------------------------------------------------------------------------------------------
# Spark: the dimension tables.
# ---------------------------------------------------------------------------------------------


def date_key_column(timestamp: Column) -> Column:
    """Spark version of ``date_key``, used by the fact tables."""
    from pyspark.sql import functions as F

    return F.date_format(timestamp, "yyyyMMdd").cast("int")


def company_key_column(code: Column) -> Column:
    """Key of a licence code: a 64-bit hash, so any code (also a new one) gets a stable key."""
    from pyspark.sql import functions as F

    return F.xxhash64(code)


def dim_date(spark: SparkSession, start: date = CALENDAR_START, end: date = CALENDAR_END) -> DataFrame:
    return spark.createDataFrame(date_rows(start, end), schema=DIM_DATE_DDL)


def dim_time(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(time_rows(), schema=DIM_TIME_DDL)


def dim_zone(zones: DataFrame) -> DataFrame:
    """Taxi zones from the TLC lookup (read with a header, so every column arrives as a string)."""
    from pyspark.sql import functions as F

    location_id = F.col("LocationID").cast("int")
    service_zone = F.col("service_zone")
    return zones.select(
        location_id.alias("zone_key"),
        location_id.alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        service_zone.alias("service_zone"),
        F.coalesce(service_zone.isin(*AIRPORT_SERVICE_ZONES), F.lit(False)).alias("is_airport"),
    )


def dim_company_rows(silver: DataFrame) -> DataFrame:
    """The four known companies plus every licence code that appears in ``silver``.

    Known companies are always present, even in a month without their trips. An unknown code is kept as
    ``Other (<code>)``, never dropped (ADR 0008), so its trips still join to a company.
    """
    from pyspark.sql import functions as F

    code = F.col("hvfhs_license_num")
    known = silver.sparkSession.createDataFrame(
        [(c,) for c in sorted(COMPANIES)], schema="hvfhs_license_num string"
    )
    seen = silver.select(code).where(code.isNotNull()).distinct()
    return (
        known.unionByName(seen)
        .distinct()
        .select(
            company_key_column(code).alias("company_key"),
            code,
            company_column().alias("company_name"),
        )
    )


@dataclass(frozen=True)
class DimensionTargets:
    """The dimension tables."""

    date: Table
    time: Table
    zone: Table
    company: Table


def build_dimensions(
    spark: SparkSession, zones: DataFrame, silver_month: DataFrame, targets: DimensionTargets
) -> None:
    """Write every dimension. Safe to run for every month: the result does not depend on how often it runs.

    Calendar, hours and zones are rebuilt completely from fixed inputs. Companies are upserted, because the
    full list of licence codes is only known across all months.
    """
    replace_table(dim_date(spark), targets.date)
    replace_table(dim_time(spark), targets.time)
    replace_table(dim_zone(zones), targets.zone)
    upsert(dim_company_rows(silver_month), targets.company, key="hvfhs_license_num", ddl=DIM_COMPANY_DDL)
