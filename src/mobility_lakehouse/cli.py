"""Command line interface: ``mobility-lakehouse <command>``.

mobility-lakehouse download --month 2025-01      # works on any OS, no Spark needed
mobility-lakehouse profile  --month 2025-01      # needs Spark: run it inside Docker
mobility-lakehouse bronze   --month 2025-01      # needs Spark: run it inside Docker
mobility-lakehouse silver   --month 2025-01      # needs Spark: run it inside Docker
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from mobility_lakehouse.tlc import (
    DATASETS,
    Month,
    download_month,
    download_zone_lookup,
    local_path,
    zone_lookup_path,
)


def _month(text: str) -> Month:
    try:
        return Month.parse(text)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from None


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--month", required=True, type=_month, help="data month, YYYY-MM")
    parser.add_argument("--dataset", default="fhvhv", choices=sorted(DATASETS), help="TLC dataset")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="local data folder")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mobility-lakehouse", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    download = commands.add_parser(
        "download", help="download one month of raw data and the taxi zone lookup (safe to rerun)"
    )
    _add_common(download)

    profile = commands.add_parser("profile", help="profile one month and write a Markdown report")
    _add_common(profile)
    profile.add_argument(
        "--report-dir", type=Path, default=Path("docs/profiling"), help="where to write the report"
    )
    bronze = commands.add_parser("bronze", help="load one month into the bronze Delta table (safe to rerun)")
    _add_common(bronze)
    _add_lakehouse(bronze)

    silver = commands.add_parser(
        "silver", help="apply the quality rules to one bronze month: silver, quarantine and rule counts"
    )
    _add_common(silver)
    _add_lakehouse(silver)
    return parser


def _add_lakehouse(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--lakehouse-dir",
        type=Path,
        default=Path("data/lakehouse"),
        help="local folder that holds the Delta tables",
    )


def table_path(lakehouse_dir: Path, layer: str, name: str) -> Path:
    return lakehouse_dir / layer / name


def bronze_table_path(lakehouse_dir: Path, dataset: str) -> Path:
    return table_path(lakehouse_dir, "bronze", f"{dataset}_trips")


def _run_download(args: argparse.Namespace) -> int:
    for result in (
        download_month(args.dataset, args.month, args.data_dir),
        download_zone_lookup(args.data_dir),
    ):
        size_mb = result.size_bytes / 1_000_000
        action = "Downloaded" if result.downloaded else "Already up to date"
        print(f"{action}: {result.path} ({size_mb:,.1f} MB)")
    return 0


def _run_profile(args: argparse.Namespace) -> int:
    from mobility_lakehouse.local_spark import create_local_spark
    from mobility_lakehouse.profiling import profile_month, write_report

    path = local_path(args.data_dir, args.dataset, args.month)
    if not path.exists():
        print(f"File not found: {path}\nRun first: mobility-lakehouse download --month {args.month}")
        return 1

    spark = create_local_spark("mobility-lakehouse-profile")
    try:
        df = spark.read.parquet(str(path))
        profile = profile_month(df, args.dataset, args.month, source_file=path.name)
    finally:
        spark.stop()

    report = write_report(profile, args.report_dir)
    print(f"Profiled {profile.row_count:,} rows. Report written to {report}")
    return 0


def _run_bronze(args: argparse.Namespace) -> int:
    from pyspark.sql import functions as F

    from mobility_lakehouse.bronze import prepare_bronze, write_bronze
    from mobility_lakehouse.local_spark import create_local_spark

    source = local_path(args.data_dir, args.dataset, args.month)
    if not source.exists():
        print(f"File not found: {source}\nRun first: mobility-lakehouse download --month {args.month}")
        return 1

    target = bronze_table_path(args.lakehouse_dir, args.dataset)
    spark = create_local_spark("mobility-lakehouse-bronze")
    try:
        raw = spark.read.parquet(str(source))
        bronze = prepare_bronze(raw, args.month, source_file=source.name)
        write_bronze(bronze, args.month, path=str(target))
        table = spark.read.format("delta").load(str(target))
        month_rows = table.where(F.col("data_month") == F.lit(args.month.start)).count()
        months = table.select("data_month").distinct().count()
    finally:
        spark.stop()

    print(f"Bronze {args.month}: {month_rows:,} rows in {target} ({months} month(s) in the table)")
    return 0


def _run_silver(args: argparse.Namespace) -> int:
    from pyspark.sql import functions as F

    from mobility_lakehouse.local_spark import create_local_spark
    from mobility_lakehouse.silver import SilverTargets, build_silver

    zones_file = zone_lookup_path(args.data_dir)
    if not zones_file.exists():
        print(f"File not found: {zones_file}\nRun first: mobility-lakehouse download --month {args.month}")
        return 1

    bronze_path = bronze_table_path(args.lakehouse_dir, args.dataset)
    name = f"{args.dataset}_trips"
    targets = SilverTargets(
        silver=str(table_path(args.lakehouse_dir, "silver", name)),
        quarantine=str(table_path(args.lakehouse_dir, "quarantine", name)),
        rule_counts=str(table_path(args.lakehouse_dir, "quality", f"{args.dataset}_rule_counts")),
    )

    spark = create_local_spark("mobility-lakehouse-silver")
    try:
        bronze = spark.read.format("delta").load(str(bronze_path))
        bronze_month = bronze.where(F.col("data_month") == F.lit(args.month.start))
        # Never replace a month with nothing: an empty input would erase that month downstream.
        if bronze_month.isEmpty():
            print(f"No bronze rows for {args.month}.")
            print(f"Run first: mobility-lakehouse bronze --month {args.month}")
            return 1
        zones = spark.read.option("header", True).csv(str(zones_file))
        result = build_silver(bronze_month, zones, args.month, targets)
    finally:
        spark.stop()

    def share(n: int) -> str:
        return f"{100 * n / result.total_rows:.4f}%"

    print(f"Silver {args.month}: {result.total_rows:,} bronze rows")
    print(f"  silver:     {result.silver_rows:,} ({share(result.silver_rows)})")
    print(f"  quarantine: {result.quarantined_rows:,} ({share(result.quarantined_rows)})")
    for count in result.rule_counts:
        label = f"{count.rule_id} {count.description}"
        print(f"  {label:<40} {count.matched_rows:>12,} ({share(count.matched_rows)}) [{count.severity}]")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    commands = {
        "download": _run_download,
        "profile": _run_profile,
        "bronze": _run_bronze,
        "silver": _run_silver,
    }
    return commands[args.command](args)
