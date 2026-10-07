"""Bronze layer: load one month of raw TLC data into a Delta table (ADR 0001, 0003, 0007).

Every run replaces exactly one month partition, so running the same month again gives the same table.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from mobility_lakehouse.contracts import (
    BRONZE_PARTITION_COLUMN,
    HVFHV_SOURCE,
    check_source_columns,
)
from mobility_lakehouse.delta_io import month_predicate, replace_month
from mobility_lakehouse.tlc import Month

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__ = ["month_predicate", "prepare_bronze", "write_bronze"]


def prepare_bronze(
    raw: DataFrame,
    month: Month,
    source_file: str,
    ingested_at: datetime | None = None,
) -> DataFrame:
    """Conform raw rows to the source contract and add bronze metadata.

    - Unknown columns stop the load (SchemaContractError).
    - Contract columns missing from the file are added as typed nulls.
    - Values keep their meaning: columns are only cast to the contract type. Spark 4 runs in ANSI mode, so a
      value that cannot be cast fails the load instead of silently becoming null.
    """
    from pyspark.sql import functions as F

    check_source_columns(raw.columns)
    present = set(raw.columns)
    loaded_at = ingested_at or datetime.now(UTC)

    columns = [
        (F.col(c.name) if c.name in present else F.lit(None)).cast(c.spark_type).alias(c.name)
        for c in HVFHV_SOURCE
    ]
    return raw.select(
        *columns,
        F.lit(source_file).alias("_source_file"),
        F.lit(loaded_at).cast("timestamp").alias("_ingested_at"),
        F.lit(month.start).alias(BRONZE_PARTITION_COLUMN),
    )


def write_bronze(
    bronze: DataFrame,
    month: Month,
    *,
    path: str | None = None,
    table: str | None = None,
) -> None:
    """Replace one month in the bronze Delta table, given either a storage path or a table name."""
    replace_month(bronze, month, path=path, table=table)
