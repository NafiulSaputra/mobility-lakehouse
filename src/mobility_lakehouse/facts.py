"""Star schema facts: one row per trip, and an hourly summary for dashboards (ADR 0014).

``fct_trips`` has the grain "one trip that passed every reject rule". It is built from one silver month, so
quarantined rows never reach it (ADR 0004, 0008), and it replaces that month only (ADR 0003).

``fct_trips_hourly`` has the grain "pickup date x pickup hour x pickup zone x company". It is built from
``fct_trips`` (not from silver), so its totals always reconcile with the trip-level facts. It stores sums and
counts, never averages: an average is computed from totals in the report (ADR 0008).

Keys point at the dimensions in ``dimensions.py``:

- ``date_key`` and ``time_key``: pickup date and hour, New York local time (ADR 0006)
- ``pickup_zone_key`` and ``dropoff_zone_key``: the same zone dimension used twice (role-playing dimension)
- ``company_key``: the licence code

The TLC data has no trip ID, and identical rows exist. ``trip_key`` is therefore a hash of every source column
plus a sequence number inside a group of identical rows, so every trip gets its own key and a rerun gives the
same keys.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mobility_lakehouse.contracts import HVFHV_SOURCE
from mobility_lakehouse.delta_io import PARTITION_COLUMN, Table, write_month
from mobility_lakehouse.dimensions import check_in_calendar, company_key_column, date_key_column
from mobility_lakehouse.gold import PASSENGER_CHARGES
from mobility_lakehouse.tlc import Month

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

SOURCE_COLUMNS = tuple(c.name for c in HVFHV_SOURCE)

# Source measures copied as they are, in the order they appear in the fact table.
TRIP_MEASURES = (
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
    "tips",
    "driver_pay",
)

FCT_TRIPS_KEYS = (
    "trip_key",
    "date_key",
    "time_key",
    "pickup_zone_key",
    "dropoff_zone_key",
    "company_key",
)
FCT_TRIPS_COLUMNS = (
    *FCT_TRIPS_KEYS,
    *TRIP_MEASURES,
    "total_fare",
    "wait_time_seconds",
    PARTITION_COLUMN,
)


def total_fare() -> Column:
    """Everything the passenger paid for one trip, rounded to cents.

    Same charges as ``passenger_paid_total`` in gold (ADR 0008), so star and gold totals agree. A missing
    charge adds 0: the CBD congestion fee is null before 2025 because it did not exist yet (ADR 0006).
    """
    from pyspark.sql import functions as F

    return F.round(sum(F.coalesce(F.col(c), F.lit(0.0)) for c in PASSENGER_CHARGES), 2)


def wait_time_seconds() -> Column:
    """Seconds from request to pickup, or null when that cannot be a real wait.

    About 1% of trips are picked up before they were requested (warning rule W001, ADR 0006). A negative wait
    is not a real wait, so it becomes null instead of pulling the average down; a missing request time is null
    too. Averages over this column therefore only describe trips with a plausible wait (ADR 0014).
    """
    from pyspark.sql import functions as F

    seconds = F.expr("timestampdiff(SECOND, request_datetime, pickup_datetime)")
    return F.when(seconds >= 0, seconds)


def trip_key() -> Column:
    """A stable, unique key per trip: hash of all source columns and the position among identical rows.

    Identical rows are interchangeable, so which copy gets sequence 1 does not matter: the set of keys is the
    same on every run.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    identical = Window.partitionBy(*SOURCE_COLUMNS).orderBy(F.lit(1))
    return F.xxhash64(*SOURCE_COLUMNS, F.row_number().over(identical))


def fct_trips(silver_month: DataFrame) -> DataFrame:
    """One row per silver trip, with dimension keys, source measures and derived measures."""
    from pyspark.sql import functions as F

    pickup = F.col("pickup_datetime")
    return silver_month.select(
        trip_key().alias("trip_key"),
        date_key_column(pickup).alias("date_key"),
        F.hour(pickup).alias("time_key"),
        F.col("PULocationID").cast("int").alias("pickup_zone_key"),
        F.col("DOLocationID").cast("int").alias("dropoff_zone_key"),
        company_key_column(F.col("hvfhs_license_num")).alias("company_key"),
        *TRIP_MEASURES,
        total_fare().alias("total_fare"),
        wait_time_seconds().alias("wait_time_seconds"),
        PARTITION_COLUMN,
    )


def build_fct_trips(silver_month: DataFrame, month: Month, target: Table) -> None:
    """Replace one month of ``fct_trips``.

    Stops before writing when the month is outside the calendar dimension, because its facts would point at
    dates that ``dim_date`` does not have.
    """
    check_in_calendar(month)
    write_month(fct_trips(silver_month), month, target)


# Grain of the hourly fact: one row per pickup date, pickup hour, pickup zone and company.
HOURLY_KEYS = ("date_key", "time_key", "pickup_zone_key", "company_key")

# Money measures, summed and rounded to cents. ``trip_miles`` and ``trip_time`` are summed separately.
MONEY_MEASURES = (
    "base_passenger_fare",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
    "tips",
    "driver_pay",
    "total_fare",
)

FCT_TRIPS_HOURLY_COLUMNS = (
    *HOURLY_KEYS,
    "trips",
    "trip_miles_sum",
    "trip_time_sum",
    *(f"{m}_sum" for m in MONEY_MEASURES),
    "wait_time_seconds_sum",
    "wait_time_trips",
    PARTITION_COLUMN,
)


def fct_trips_hourly(facts: DataFrame) -> DataFrame:
    """Aggregate trip facts to date x hour x pickup zone x company.

    ``wait_time_trips`` counts only the trips that have a wait time, so the report computes the average wait
    as ``wait_time_seconds_sum / wait_time_trips``; dividing by ``trips`` would count trips without a valid
    wait as zero-second waits. A money column that is null for every trip of a group stays null (the CBD
    congestion fee before 2025, ADR 0006).
    """
    from pyspark.sql import functions as F

    return (
        facts.groupBy(*HOURLY_KEYS, PARTITION_COLUMN)
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.sum("trip_miles"), 2).alias("trip_miles_sum"),
            F.sum("trip_time").alias("trip_time_sum"),
            *(F.round(F.sum(m), 2).alias(f"{m}_sum") for m in MONEY_MEASURES),
            F.sum("wait_time_seconds").alias("wait_time_seconds_sum"),
            F.count("wait_time_seconds").alias("wait_time_trips"),
        )
        .select(*FCT_TRIPS_HOURLY_COLUMNS)
    )


def build_fct_trips_hourly(facts_month: DataFrame, month: Month, target: Table) -> None:
    """Replace one month of ``fct_trips_hourly`` from that month of ``fct_trips``."""
    check_in_calendar(month)
    write_month(fct_trips_hourly(facts_month), month, target)
