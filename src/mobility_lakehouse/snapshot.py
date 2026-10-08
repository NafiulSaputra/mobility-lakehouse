"""Serving snapshot: the tables the API reads, written as plain Parquet (ADR 0011).

The API does not query Databricks. A snapshot step copies one month of the gold tables and the quality counts
into Parquet folders partitioned by ``data_month``. Any engine can read them; the API uses DuckDB.

Writing a month replaces only that month's partition (dynamic partition overwrite), so the snapshot is
idempotent in the same way as the Delta tables (ADR 0003).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mobility_lakehouse.delta_io import PARTITION_COLUMN

if TYPE_CHECKING:
    from datetime import date

    from pyspark.sql import DataFrame, SparkSession

# Table folders in the snapshot. The API reads exactly these names.
SNAPSHOT_TABLES = (
    "daily_company_trips",
    "hourly_pickup_zones",
    "monthly_driver_economics",
    "rule_counts",
    "quality_months",
)

QUALITY_MONTHS_DDL = "data_month date, rows_checked bigint, rows_quarantined bigint"


def quality_months_frame(
    spark: SparkSession, month_start: date, rows_checked: int, rows_quarantined: int
) -> DataFrame:
    """One row per month: how many bronze rows were checked and how many were quarantined."""
    return spark.createDataFrame([(month_start, rows_checked, rows_quarantined)], schema=QUALITY_MONTHS_DDL)


def write_snapshot_table(df: DataFrame, root: str, name: str) -> None:
    """Replace the months present in ``df`` in one snapshot table. Other months are not touched."""
    (
        df.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(PARTITION_COLUMN)
        .parquet(f"{root}/{name}")
    )
