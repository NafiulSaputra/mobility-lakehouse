"""Shared Delta Lake access: where a table lives, and how it is rewritten (ADR 0003, 0009, 0014).

Every write in this project is idempotent, and each strategy is implemented once and tested once:

- ``replace_month``: replace one ``data_month`` partition. Used by every layer and by the fact tables.
- ``replace_table``: replace a whole small table that is rebuilt from complete inputs (calendar, hours,
  taxi zones).
- ``upsert``: insert new rows and update existing ones by key, for a dimension that only becomes complete
  across months (companies seen in the data).

A ``Table`` is either a storage path (local Docker) or a Unity Catalog table name (Databricks), so the same
pipeline code runs in both places.
"""

from __future__ import annotations

import os
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

    def sql_identifier(self) -> str:
        """How Spark SQL refers to this table: ``delta.`<path>``` or the catalog name.

        Spark SQL only treats ``delta.`<path>``` as a path when it is absolute; a relative path such as the
        local ``data/lakehouse/...`` is read as a table in a schema called ``delta``. So local paths are
        made absolute here. Paths with a scheme (``s3://``, ``dbfs:/``) are already absolute.
        """
        if self.path is not None:
            path = self.path if ":" in self.path else os.path.abspath(self.path)
            return f"delta.`{path}`"
        return str(self.name)


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


def replace_table(df: DataFrame, target: Table) -> None:
    """Replace a whole, unpartitioned table with ``df``.

    Only for small tables that are rebuilt completely from fixed inputs on every run, such as the calendar and
    the taxi zone dimensions (ADR 0014). Partitioned data must use ``replace_month``.
    """
    writer = (
        df.write.format("delta")
        .mode("overwrite")  # noqa: DE002 - the whole table is rebuilt from complete inputs on every run
        .option("overwriteSchema", "true")
    )
    if target.path is not None:
        writer.save(target.path)
    else:
        writer.saveAsTable(str(target.name))


def upsert(df: DataFrame, target: Table, *, key: str, ddl: str) -> None:
    """Insert the rows of ``df`` that are new by ``key`` and update the ones that exist (MERGE).

    The table is created with ``ddl`` the first time. Rows that are not in ``df`` are kept, so a dimension can
    grow month by month. Running the same upsert twice gives the same table. ``key`` and ``ddl`` come from
    code, never from user input.
    """
    spark = df.sparkSession
    table = target.sql_identifier()
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({ddl}) USING DELTA")
    source = "upsert_source"
    df.createOrReplaceTempView(source)
    try:
        spark.sql(
            f"MERGE INTO {table} AS t USING {source} AS s ON t.{key} = s.{key} "
            "WHEN MATCHED THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        spark.catalog.dropTempView(source)
