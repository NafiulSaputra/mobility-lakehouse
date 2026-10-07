"""Command line interface: ``mobility-lakehouse <command>``.

    mobility-lakehouse download --month 2025-01      # works on any OS, no Spark needed
    mobility-lakehouse profile  --month 2025-01      # needs Spark: run it inside Docker
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from mobility_lakehouse.tlc import DATASETS, Month, download_month, local_path


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

    download = commands.add_parser("download", help="download one month of raw data (safe to rerun)")
    _add_common(download)

    profile = commands.add_parser("profile", help="profile one month and write a Markdown report")
    _add_common(profile)
    profile.add_argument(
        "--report-dir", type=Path, default=Path("docs/profiling"), help="where to write the report"
    )
    return parser


def _run_download(args: argparse.Namespace) -> int:
    result = download_month(args.dataset, args.month, args.data_dir)
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


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "download":
        return _run_download(args)
    return _run_profile(args)
