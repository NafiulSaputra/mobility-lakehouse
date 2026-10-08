"""Shared Delta Lake access: where a table lives, and replacing exactly one month (ADR 0003, 0009).

Every table in this project is partitioned by ``data_month`` and written through ``replace_month``, so
idempotency is implemented once and tested once. A ``Table`` is either a storage path (local Docker) or a
Unity Catalog table name (Databricks), so the same pipeline code runs in both places.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from mobility_lakehouse.tlc import Month

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

PARTITION_COLUMN = "data_month"


@dataclass(frozen=True)
class Table:
    """A Delta table, given by exactly one of a storage path or a catalog table name."""

    path: str | None = None
    name: str | None = None

    def __post_init__(self) -> None:
        if (self.path is None) == (self.name is None):
            raise ValueError("a Table needs exactly one of path or name")

    def __str__(self) -> str:
        return self.path if self.path is not None else str(self.name)

    def read(self, spark: SparkSession) -> DataFrame:
        if self.path is not None:
            return spark.read.format("delta").load(self.path)
        return spark.read.table(str(self.name))


def month_predicate(month: Month) -> str:
    """The replaceWhere condition that selects one month partition."""
    return f"{PARTITION_COLUMN} = DATE'{month.start.isoformat()}'"


def replace_month(
    df: DataFrame,
    month: Month,
    *,
    path: str | None = None,
    table: str | None = None,
) -> None:
    """Replace one month of a Delta table, given either a storage path or a table name.

    Rows of other months are not touched. Writing the same month twice gives the same table.
    """
    if (path is None) == (table is None):
        raise ValueError("give exactly one of path or table")

    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", month_predicate(month))
        .partitionBy(PARTITION_COLUMN)
    )
    if path is not None:
        writer.save(path)
    else:
        writer.saveAsTable(table)


def write_month(df: DataFrame, month: Month, target: Table) -> None:
    """Replace one month of ``target``, wherever it lives."""
    replace_month(df, month, path=target.path, table=target.name)
