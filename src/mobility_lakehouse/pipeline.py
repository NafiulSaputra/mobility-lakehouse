"""The pipeline steps, shared by the local CLI and the Databricks job (ADR 0009).

A ``Layout`` says where the raw files and the tables live: local folders inside Docker, or a Unity Catalog
schema and volume on Databricks. The steps themselves are identical in both places.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mobility_lakehouse.delta_io import PARTITION_COLUMN, Table
from mobility_lakehouse.gold import GoldTargets
from mobility_lakehouse.silver import SilverResult, SilverTargets
from mobility_lakehouse.tlc import Month, file_name

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class MissingInputError(RuntimeError):
    """A step was asked to run before the step it depends on produced data for that month."""


@dataclass(frozen=True)
class Layout:
    """Where every input file and table of one dataset lives."""

    dataset: str
    raw_dir: str  # folder that holds <dataset>/<file> and reference/taxi_zone_lookup.csv
    bronze: Table
    silver: SilverTargets
    gold: GoldTargets
    persist: bool = True  # Databricks serverless does not support DataFrame caching

    def raw_file(self, month: Month) -> str:
        return f"{self.raw_dir}/{self.dataset}/{file_name(self.dataset, month)}"

    @property
    def zones_file(self) -> str:
        return f"{self.raw_dir}/reference/taxi_zone_lookup.csv"


def local_layout(data_dir: Path, lakehouse_dir: Path, dataset: str = "fhvhv") -> Layout:
    """Folders used by the local CLI inside Docker."""

    def table(layer: str, name: str) -> Table:
        return Table(path=(lakehouse_dir / layer / f"{dataset}_{name}").as_posix())

    return Layout(
        dataset=dataset,
        raw_dir=(data_dir / "raw").as_posix(),
        bronze=table("bronze", "trips"),
        silver=SilverTargets(
            silver=table("silver", "trips"),
            quarantine=table("quarantine", "trips"),
            rule_counts=table("quality", "rule_counts"),
        ),
        gold=GoldTargets(
            daily_company_trips=table("gold", "daily_company_trips"),
            hourly_pickup_zones=table("gold", "hourly_pickup_zones"),
            monthly_driver_economics=table("gold", "monthly_driver_economics"),
        ),
        persist=True,
    )


def catalog_layout(catalog: str, schema: str, dataset: str = "fhvhv", volume: str = "raw") -> Layout:
    """Unity Catalog tables and a volume for raw files, used on Databricks."""

    def table(layer: str, name: str) -> Table:
        return Table(name=f"{catalog}.{schema}.{layer}_{dataset}_{name}")

    return Layout(
        dataset=dataset,
        raw_dir=f"/Volumes/{catalog}/{schema}/{volume}",
        bronze=table("bronze", "trips"),
        silver=SilverTargets(
            silver=table("silver", "trips"),
            quarantine=table("quarantine", "trips"),
            rule_counts=table("quality", "rule_counts"),
        ),
        gold=GoldTargets(
            daily_company_trips=table("gold", "daily_company_trips"),
            hourly_pickup_zones=table("gold", "hourly_pickup_zones"),
            monthly_driver_economics=table("gold", "monthly_driver_economics"),
        ),
        persist=False,
    )


def _month_of(df: DataFrame, month: Month) -> DataFrame:
    from pyspark.sql import functions as F

    return df.where(F.col(PARTITION_COLUMN) == F.lit(month.start))


def run_bronze(spark: SparkSession, layout: Layout, month: Month) -> int:
    """Load one raw month into bronze. Returns the number of bronze rows for that month."""
    from mobility_lakehouse.bronze import prepare_bronze
    from mobility_lakehouse.delta_io import write_month

    source = layout.raw_file(month)
    raw = spark.read.parquet(source)
    bronze = prepare_bronze(raw, month, source_file=source.rsplit("/", 1)[-1])
    write_month(bronze, month, layout.bronze)
    return _month_of(layout.bronze.read(spark), month).count()


def run_silver(spark: SparkSession, layout: Layout, month: Month) -> SilverResult:
    """Apply the quality rules to one bronze month."""
    from mobility_lakehouse.silver import build_silver

    bronze_month = _month_of(layout.bronze.read(spark), month)
    # Never replace a month with nothing: an empty input would erase that month downstream.
    if bronze_month.isEmpty():
        raise MissingInputError(f"no bronze rows for {month}; run the bronze step first")
    zones = spark.read.option("header", True).csv(layout.zones_file)
    return build_silver(bronze_month, zones, month, layout.silver, persist=layout.persist)


@dataclass(frozen=True)
class GoldSummary:
    rows_per_table: dict[str, int]
    driver_economics: list[dict[str, Any]] = field(default_factory=list)


def run_gold(spark: SparkSession, layout: Layout, month: Month) -> GoldSummary:
    """Rebuild every gold table for one silver month."""
    from pyspark.sql import functions as F

    from mobility_lakehouse.gold import build_gold

    silver_month = _month_of(layout.silver.silver.read(spark), month)
    if silver_month.isEmpty():
        raise MissingInputError(f"no silver rows for {month}; run the silver step first")
    build_gold(silver_month, month, layout.gold)

    rows = {name: _month_of(table.read(spark), month).count() for name, table in vars(layout.gold).items()}
    economics = _month_of(layout.gold.monthly_driver_economics.read(spark), month)
    top = economics.orderBy(F.col("trips").desc()).limit(10).collect()
    return GoldSummary(rows, [row.asDict() for row in top])


# ---------------------------------------------------------------------------------------------
# Summaries: plain Python, shared by the CLI and the Databricks job logs.
# ---------------------------------------------------------------------------------------------


def silver_summary(month: Month, result: SilverResult) -> list[str]:
    def share(n: int) -> str:
        return f"{100 * n / result.total_rows:.4f}%" if result.total_rows else "n/a"

    lines = [
        f"Silver {month}: {result.total_rows:,} bronze rows",
        f"  silver:     {result.silver_rows:,} ({share(result.silver_rows)})",
        f"  quarantine: {result.quarantined_rows:,} ({share(result.quarantined_rows)})",
    ]
    for count in result.rule_counts:
        label = f"{count.rule_id} {count.description}"
        matched = f"{count.matched_rows:>12,} ({share(count.matched_rows)})"
        lines.append(f"  {label:<40} {matched} [{count.severity}]")
    return lines


def gold_summary(month: Month, summary: GoldSummary) -> list[str]:
    lines = [f"Gold {month}:"]
    lines += [f"  {name:<26} {rows:>9,} rows" for name, rows in summary.rows_per_table.items()]
    lines.append("  Driver economics:")
    for row in summary.driver_economics:
        lines.append(
            f"    {row['company']:<14} {row['trips']:>11,} trips  "
            f"${row['driver_pay_per_mile']}/mile  ${row['driver_pay_per_minute']}/minute  "
            f"driver share of base fare {row['driver_share_of_base_fare']}"
        )
    return lines
