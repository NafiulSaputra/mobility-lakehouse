"""Schema contracts: the columns and types each layer accepts (ADR 0007).

Kept as plain Python data, so the contract can be read, tested and documented without Spark.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


class SchemaContractError(ValueError):
    """The source data does not match the declared contract."""


@dataclass(frozen=True)
class Column:
    name: str
    spark_type: str  # Spark SQL type name, for example "timestamp_ntz"
    since: str | None = None  # first month the TLC published this column, if not from the start


# HVFHV trip records as published by the TLC (data dictionary and profiling of 2024-06 and 2025-01).
HVFHV_SOURCE: tuple[Column, ...] = (
    Column("hvfhs_license_num", "string"),
    Column("dispatching_base_num", "string"),
    Column("originating_base_num", "string"),
    Column("request_datetime", "timestamp_ntz"),
    Column("on_scene_datetime", "timestamp_ntz"),
    Column("pickup_datetime", "timestamp_ntz"),
    Column("dropoff_datetime", "timestamp_ntz"),
    Column("PULocationID", "int"),
    Column("DOLocationID", "int"),
    Column("trip_miles", "double"),
    Column("trip_time", "bigint"),
    Column("base_passenger_fare", "double"),
    Column("tolls", "double"),
    Column("bcf", "double"),
    Column("sales_tax", "double"),
    Column("congestion_surcharge", "double"),
    Column("airport_fee", "double"),
    Column("tips", "double"),
    Column("driver_pay", "double"),
    Column("shared_request_flag", "string"),
    Column("shared_match_flag", "string"),
    Column("access_a_ride_flag", "string"),
    Column("wav_request_flag", "string"),
    Column("wav_match_flag", "string"),
    Column("cbd_congestion_fee", "double", since="2025-01"),
)

# Columns bronze adds to every row.
BRONZE_METADATA: tuple[Column, ...] = (
    Column("_source_file", "string"),
    Column("_ingested_at", "timestamp"),
    Column("data_month", "date"),
)

BRONZE_PARTITION_COLUMN = "data_month"


def check_source_columns(actual: Sequence[str], contract: Sequence[Column] = HVFHV_SOURCE) -> list[str]:
    """Compare a file's columns with the contract.

    Returns the contract columns missing from the file; they are loaded as null.
    Raises SchemaContractError if the file has columns the contract does not know: a new source column is a
    change that must be reviewed and added to the contract on purpose, never loaded silently.
    """
    expected = {c.name for c in contract}
    unknown = [name for name in actual if name not in expected]
    if unknown:
        raise SchemaContractError(
            f"source has columns that are not in the contract: {', '.join(unknown)}. "
            "Review the change, then add them to contracts.py (see ADR 0007)."
        )
    present = set(actual)
    return [c.name for c in contract if c.name not in present]


def ddl(columns: Sequence[Column]) -> str:
    """Spark DDL string for a list of columns, for example "a string, b int"."""
    return ", ".join(f"`{c.name}` {c.spark_type}" for c in columns)
