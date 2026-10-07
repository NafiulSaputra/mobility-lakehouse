"""Shared Delta Lake write: replace exactly one month partition (ADR 0003).

Every table in this project is partitioned by ``data_month`` and written through this function, so
idempotency is implemented once and tested once.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mobility_lakehouse.tlc import Month

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

PARTITION_COLUMN = "data_month"


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
