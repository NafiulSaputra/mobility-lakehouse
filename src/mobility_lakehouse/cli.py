"""Command line interface: ``mobility-lakehouse <command>``.

mobility-lakehouse download --month 2025-01      # works on any OS, no Spark needed
mobility-lakehouse profile  --month 2025-01      # needs Spark: run it inside Docker
mobility-lakehouse bronze   --month 2025-01      # needs Spark: run it inside Docker
mobility-lakehouse silver   --month 2025-01      # needs Spark: run it inside Docker
mobility-lakehouse gold     --month 2025-01      # needs Spark: run it inside Docker
mobility-lakehouse snapshot --month 2025-01      # needs Spark: Parquet tables for the API (data/snapshot)
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from mobility_lakehouse.pipeline import Layout, MissingInputError, local_layout
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

    gold = commands.add_parser("gold", help="rebuild the gold tables for one silver month (safe to rerun)")
    _add_common(gold)
    _add_lakehouse(gold)

    snapshot = commands.add_parser(
        "snapshot", help="write one gold month as Parquet for the API, in <data-dir>/snapshot (safe to rerun)"
    )
    _add_common(snapshot)
    _add_lakehouse(snapshot)
    return parser


def _add_lakehouse(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--lakehouse-dir",
        type=Path,
        default=Path("data/lakehouse"),
        help="local folder that holds the Delta tables",
    )


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


def _local(args: argparse.Namespace) -> Layout:
    return local_layout(args.data_dir, args.lakehouse_dir, args.dataset)


def _run_bronze(args: argparse.Namespace) -> int:
    from mobility_lakehouse.local_spark import create_local_spark
    from mobility_lakehouse.pipeline import run_bronze

    source = local_path(args.data_dir, args.dataset, args.month)
    if not source.exists():
        print(f"File not found: {source}\nRun first: mobility-lakehouse download --month {args.month}")
        return 1

    layout = _local(args)
    spark = create_local_spark("mobility-lakehouse-bronze")
    try:
        month_rows = run_bronze(spark, layout, args.month)
        months = layout.bronze.read(spark).select("data_month").distinct().count()
    finally:
        spark.stop()

    print(f"Bronze {args.month}: {month_rows:,} rows in {layout.bronze} ({months} month(s) in the table)")
    return 0


def _run_silver(args: argparse.Namespace) -> int:
    from mobility_lakehouse.local_spark import create_local_spark
    from mobility_lakehouse.pipeline import run_silver, silver_summary

    zones_file = zone_lookup_path(args.data_dir)
    if not zones_file.exists():
        print(f"File not found: {zones_file}\nRun first: mobility-lakehouse download --month {args.month}")
        return 1

    spark = create_local_spark("mobility-lakehouse-silver")
    try:
        result = run_silver(spark, _local(args), args.month)
    except MissingInputError as error:
        print(f"Cannot build silver: {error}")
        return 1
    finally:
        spark.stop()

    print("\n".join(silver_summary(args.month, result)))
    return 0


def _run_gold(args: argparse.Namespace) -> int:
    from mobility_lakehouse.local_spark import create_local_spark
    from mobility_lakehouse.pipeline import gold_summary, run_gold

    spark = create_local_spark("mobility-lakehouse-gold")
    try:
        summary = run_gold(spark, _local(args), args.month)
    except MissingInputError as error:
        print(f"Cannot build gold: {error}")
        return 1
    finally:
        spark.stop()

    print("\n".join(gold_summary(args.month, summary)))
    return 0


def _run_snapshot(args: argparse.Namespace) -> int:
    from mobility_lakehouse.local_spark import create_local_spark
    from mobility_lakehouse.pipeline import run_snapshot, snapshot_summary

    layout = _local(args)
    spark = create_local_spark("mobility-lakehouse-snapshot")
    try:
        rows = run_snapshot(spark, layout, args.month)
    except MissingInputError as error:
        print(f"Cannot write the snapshot: {error}")
        return 1
    finally:
        spark.stop()

    print("\n".join(snapshot_summary(args.month, rows, layout.snapshot_dir)))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    commands = {
        "download": _run_download,
        "profile": _run_profile,
        "bronze": _run_bronze,
        "silver": _run_silver,
        "gold": _run_gold,
        "snapshot": _run_snapshot,
    }
    return commands[args.command](args)
