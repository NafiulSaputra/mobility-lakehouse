"""Silver layer: apply the quality rules to one bronze month (ADR 0004, 0006).

One bronze month becomes three outputs, each replacing that month only:

- silver: rows that pass every reject rule, with taxi zone names and the warnings they triggered
- quarantine: rows that failed at least one reject rule, with the failed rule IDs
- rule counts: one row per rule with the number of rows that matched it
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from mobility_lakehouse.contracts import HVFHV_SOURCE
from mobility_lakehouse.delta_io import PARTITION_COLUMN, replace_month
from mobility_lakehouse.quality import (
    CANDIDATE_KEY,
    CANDIDATE_KEY_ROWS,
    RULES,
    Severity,
    rules_of,
)
from mobility_lakehouse.tlc import Month

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

FAILED_RULES = "failed_rules"
WARNINGS = "quality_warnings"
ZONE_COLUMNS = ("pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone")
SOURCE_COLUMNS = tuple(c.name for c in HVFHV_SOURCE)
SILVER_COLUMNS = (*SOURCE_COLUMNS, *ZONE_COLUMNS, WARNINGS, "_source_file", PARTITION_COLUMN)
QUARANTINE_COLUMNS = (*SOURCE_COLUMNS, FAILED_RULES, WARNINGS, "_source_file", PARTITION_COLUMN)
RULE_COUNTS_DDL = (
    "data_month date, rule_id string, severity string, description string, "
    "matched_rows bigint, total_rows bigint"
)


@dataclass(frozen=True)
class RuleCount:
    rule_id: str
    severity: str
    description: str
    matched_rows: int


@dataclass(frozen=True)
class SilverResult:
    total_rows: int
    silver_rows: int
    quarantined_rows: int
    rule_counts: tuple[RuleCount, ...]


def _matched_rule_ids(severity: Severity) -> Column:
    """Array of the IDs of the rules of one severity that a row violates (nulls count as not violated)."""
    from pyspark.sql import functions as F

    flags = [F.when(F.expr(f"coalesce({r.condition}, false)"), F.lit(r.id)) for r in rules_of(severity)]
    return F.array_compact(F.array(*flags))


def evaluate(bronze: DataFrame, zones: DataFrame) -> DataFrame:
    """Add rule results and zone names to bronze rows. Nothing is filtered yet."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    key = Window.partitionBy(*CANDIDATE_KEY)
    lookup = zones.select(
        F.col("LocationID").cast("int").alias("zone_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
    )
    pickup = lookup.select(
        F.col("zone_id").alias("_pu_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
    )
    dropoff = lookup.select(
        F.col("zone_id").alias("_do_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
    )

    checked = (
        bronze.withColumn(CANDIDATE_KEY_ROWS, F.count(F.lit(1)).over(key))
        .withColumn(FAILED_RULES, _matched_rule_ids(Severity.REJECT))
        .withColumn(WARNINGS, _matched_rule_ids(Severity.WARNING))
    )
    # The zone lookup has a few hundred rows: broadcast it instead of shuffling the trips.
    return (
        checked.join(F.broadcast(pickup), checked["PULocationID"] == pickup["_pu_id"], "left")
        .join(F.broadcast(dropoff), checked["DOLocationID"] == dropoff["_do_id"], "left")
        .drop("_pu_id", "_do_id")
    )


def silver_rows(evaluated: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return evaluated.where(F.size(FAILED_RULES) == 0).select(*SILVER_COLUMNS)


def quarantine_rows(evaluated: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return evaluated.where(F.size(FAILED_RULES) > 0).select(*QUARANTINE_COLUMNS)


def count_rules(evaluated: DataFrame) -> SilverResult:
    """Count rows per rule in one pass. A global aggregate returns exactly one row."""
    from pyspark.sql import functions as F

    def matched(column: str, rule_id: str) -> Column:
        return F.sum(F.array_contains(F.col(column), rule_id).cast("long"))

    exprs = [
        F.count(F.lit(1)).alias("total"),
        F.sum((F.size(FAILED_RULES) > 0).cast("long")).alias("quarantined"),
    ]
    for rule in RULES:
        column = FAILED_RULES if rule.severity is Severity.REJECT else WARNINGS
        exprs.append(matched(column, rule.id).alias(rule.id))

    row = evaluated.agg(*exprs).first()
    total = int(row["total"])
    quarantined = int(row["quarantined"] or 0)
    counts = tuple(
        RuleCount(rule.id, rule.severity.value, rule.description, int(row[rule.id] or 0)) for rule in RULES
    )
    return SilverResult(total, total - quarantined, quarantined, counts)


def rule_counts_frame(spark: SparkSession, result: SilverResult, month_start: date) -> DataFrame:
    rows = [
        (month_start, c.rule_id, c.severity, c.description, c.matched_rows, result.total_rows)
        for c in result.rule_counts
    ]
    return spark.createDataFrame(rows, schema=RULE_COUNTS_DDL)


@dataclass(frozen=True)
class SilverTargets:
    """Storage paths of the three tables written for each month."""

    silver: str
    quarantine: str
    rule_counts: str


def build_silver(
    bronze_month: DataFrame,
    zones: DataFrame,
    month: Month,
    targets: SilverTargets,
) -> SilverResult:
    """Run the quality rules on one bronze month and replace that month in all three tables.

    The evaluated rows are persisted to disk once, so the window and the rules run a single time even though
    three tables are written. Disk instead of memory keeps a 20-million-row month within a laptop's memory.
    """
    from pyspark import StorageLevel

    evaluated = evaluate(bronze_month, zones).persist(StorageLevel.DISK_ONLY)
    try:
        result = count_rules(evaluated)
        replace_month(silver_rows(evaluated), month, path=targets.silver)
        replace_month(quarantine_rows(evaluated), month, path=targets.quarantine)
        counts = rule_counts_frame(evaluated.sparkSession, result, month.start)
        replace_month(counts, month, path=targets.rule_counts)
    finally:
        evaluated.unpersist()
    return result
