"""Entry point for the Databricks job: ``mobility-lakehouse-job <step> --month YYYY-MM`` (ADR 0009).

Each job task runs one step against Unity Catalog tables. The runtime provides the Spark session. Any
problem raises an exception, so the task fails visibly instead of finishing "successfully" with no data.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from mobility_lakehouse.pipeline import (
    catalog_layout,
    gold_summary,
    run_bronze,
    run_gold,
    run_silver,
    silver_summary,
)
from mobility_lakehouse.tlc import DATASETS, Month

STEPS = ("bronze", "silver", "gold")


def _month(text: str) -> Month:
    try:
        return Month.parse(text)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mobility-lakehouse-job", description="Run one pipeline step.")
    parser.add_argument("step", choices=STEPS)
    parser.add_argument("--month", required=True, type=_month, help="data month, YYYY-MM")
    parser.add_argument("--catalog", default="workspace", help="Unity Catalog catalog")
    parser.add_argument("--schema", default="mobility", help="Unity Catalog schema")
    parser.add_argument("--volume", default="raw", help="volume in the schema that holds the raw files")
    parser.add_argument("--dataset", default="fhvhv", choices=sorted(DATASETS))
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    from pyspark.sql import SparkSession

    args = build_parser().parse_args(argv)
    layout = catalog_layout(args.catalog, args.schema, args.dataset, args.volume)
    spark = SparkSession.builder.getOrCreate()

    if args.step == "bronze":
        rows = run_bronze(spark, layout, args.month)
        if rows == 0:
            raise RuntimeError(f"bronze {args.month} is empty after loading {layout.raw_file(args.month)}")
        print(f"Bronze {args.month}: {rows:,} rows in {layout.bronze}")
    elif args.step == "silver":
        print("\n".join(silver_summary(args.month, run_silver(spark, layout, args.month))))
    else:
        print("\n".join(gold_summary(args.month, run_gold(spark, layout, args.month))))
