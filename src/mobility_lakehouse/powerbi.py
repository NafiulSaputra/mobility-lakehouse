"""Power BI export: the star schema as one Parquet file per table (ADR 0014).

Power BI Desktop imports a single Parquet file in a few clicks, so every table becomes exactly one file,
``<out_dir>/<table>.parquet``. The sizes are chosen to stay workable on a laptop:

- every dimension, with ``dim_date`` limited to the full calendar years that have data,
- ``fct_trips_hourly`` for every month that is built (a few hundred thousand rows per month),
- ``fct_trips`` for one month only (trip grain is about 20 million rows per month).

The export is rebuilt completely on every run, so running it twice gives the same files.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Import order in Power BI: dimensions first, then the facts that point at them.
POWERBI_TABLES = ("dim_date", "dim_time", "dim_zone", "dim_company", "fct_trips_hourly", "fct_trips")

# trip_key identifies a trip inside the lakehouse. The dashboard never uses it, and a unique 64-bit number on
# every row is the most expensive kind of column for Power BI to store.
TRIP_COLUMNS_NOT_EXPORTED = ("trip_key",)


@dataclass(frozen=True)
class ExportedFile:
    """One file of the export, measured after writing."""

    name: str
    path: str
    rows: int
    size_bytes: int

    @property
    def size_mb(self) -> float:
        return self.size_bytes / 1_000_000


def write_single_parquet(df: DataFrame, out_dir: str, name: str) -> str:
    """Write ``df`` as exactly one file, ``<out_dir>/<name>.parquet``, replacing any earlier file.

    Spark always writes a folder of part files. The data goes to a staging folder as a single part, which is
    then moved into place. Works on local disks and on Unity Catalog volumes (``/Volumes/...``).
    """
    os.makedirs(out_dir, exist_ok=True)
    staging = f"{out_dir}/_staging_{name}"
    target = f"{out_dir}/{name}.parquet"

    (
        df.coalesce(1)
        .write.mode("overwrite")  # noqa: DE002 - the export file is rebuilt completely on every run
        .parquet(staging)
    )
    parts = sorted(entry for entry in os.listdir(staging) if entry.endswith(".parquet"))
    if len(parts) != 1:
        raise RuntimeError(f"expected one Parquet part in {staging}, found {len(parts)}")
    os.replace(f"{staging}/{parts[0]}", target)
    shutil.rmtree(staging)
    return target


def export_files(frames: dict[str, DataFrame], out_dir: str) -> list[ExportedFile]:
    """Write every frame as one Parquet file, then read each file back to report its real rows and size."""
    files = []
    for name, frame in frames.items():
        path = write_single_parquet(frame, out_dir, name)
        rows = frame.sparkSession.read.parquet(path).count()
        files.append(ExportedFile(name, path, rows, os.path.getsize(path)))
    return files
