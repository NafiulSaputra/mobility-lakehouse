"""Command line interface: ``mobility-lakehouse <command>``.

mobility-lakehouse download --month 2025-01      # works on any OS, no Spark needed
mobility-lakehouse profile  --month 2025-01      # needs Spark: run it inside Docker
mobility-lakehouse bronze   --month 2025-01      # needs Spark: run it inside Docker
mobility-lakehouse silver   --month 2025-01      # needs Spark: run it inside Docker
mobility-lakehouse gold     --month 2025-01      # needs Spark: run it inside Docker
mobility-lakehouse snapshot --month 2025-01      # needs Spark: Parquet tables for the API (data/snapshot)

Streaming (ADR 0012), inside Docker with Redpanda running (docker compose up -d redpanda):
mobility-lakehouse replay-extract --date 2025-01-15   # one day of silver trips -> event files
mobility-lakehouse replay --date 2025-01-15           # event files -> Redpanda, with late events
mobility-lakehouse stream --until-caught-up           # Redpanda -> trips per zone and 15 minutes (Delta)
mobility-lakehouse reconcile --date 2025-01-15        # stream vs batch gold, report in docs/reports
mobility-lakehouse stream-reset                       # delete the topic, stream results and checkpoint
"""

from __future__ import annotations

import argparse
import uuid
from collections.abc import Sequence
from datetime import UTC, date, datetime
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


def _date(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid date {text!r}: expected YYYY-MM-DD") from None


def _add_streaming(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="local data folder")
    _add_lakehouse(parser)


def _add_kafka(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bootstrap", default="redpanda:9092", help="Kafka/Redpanda bootstrap servers")
    parser.add_argument("--topic", default="trips", help="topic with the trip events")


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

    extract = commands.add_parser(
        "replay-extract", help="write the silver trips of one day as replay events (safe to rerun)"
    )
    extract.add_argument("--date", required=True, type=_date, help="pickup day, YYYY-MM-DD")
    _add_streaming(extract)

    replay = commands.add_parser("replay", help="send one day of events to Redpanda, faster than real time")
    replay.add_argument("--date", required=True, type=_date, help="pickup day, YYYY-MM-DD")
    replay.add_argument(
        "--speed", type=float, default=600.0, help="times faster than real time (default 600)"
    )
    replay.add_argument("--late-fraction", type=float, default=0.02, help="share of events sent late")
    replay.add_argument("--max-delay-minutes", type=float, default=90.0, help="longest delay of a late event")
    replay.add_argument(
        "--seed", type=int, default=7, help="random seed: the same seed gives the same replay"
    )
    _add_streaming(replay)
    _add_kafka(replay)

    stream = commands.add_parser(
        "stream", help="count trips per pickup zone and 15-minute window from Redpanda"
    )
    stream.add_argument(
        "--until-caught-up",
        action="store_true",
        help="process the events already in the topic, then stop (default: run live until Ctrl+C)",
    )
    stream.add_argument("--max-offsets-per-trigger", type=int, default=50_000, help="events per micro-batch")
    _add_streaming(stream)
    _add_kafka(stream)

    reconcile = commands.add_parser(
        "reconcile", help="compare the stream with batch gold for one replayed day and write a report"
    )
    reconcile.add_argument("--date", required=True, type=_date, help="replayed day, YYYY-MM-DD")
    reconcile.add_argument(
        "--report-dir", type=Path, default=Path("docs/reports"), help="where to write the Markdown report"
    )
    _add_streaming(reconcile)

    reset = commands.add_parser(
        "stream-reset", help="delete the topic, the stream results, the run log and the checkpoint"
    )
    _add_streaming(reset)
    _add_kafka(reset)
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


def _run_replay_extract(args: argparse.Namespace) -> int:
    from mobility_lakehouse.local_spark import create_local_spark
    from mobility_lakehouse.streaming import extract_replay_day, local_streaming_paths

    layout = local_layout(args.data_dir, args.lakehouse_dir)
    paths = local_streaming_paths(args.data_dir, args.lakehouse_dir)
    spark = create_local_spark("mobility-lakehouse-replay-extract")
    try:
        rows = extract_replay_day(spark, layout.silver.silver, args.date, paths.events_dir)
    finally:
        spark.stop()

    if rows == 0:
        print(f"No silver trips on {args.date}. Build silver for {args.date:%Y-%m} first.")
        return 1
    print(f"Replay events {args.date}: {rows:,} trips written to {paths.events_dir}")
    return 0


def _run_replay(args: argparse.Namespace) -> int:
    from mobility_lakehouse.replay import KafkaSender, plan_replay, read_events, replay, summarize_plan
    from mobility_lakehouse.streaming import local_streaming_paths

    paths = local_streaming_paths(args.data_dir, args.lakehouse_dir)
    try:
        events = read_events(Path(paths.events_dir), args.date)
    except FileNotFoundError as error:
        print(error)
        return 1

    planned = plan_replay(events, args.late_fraction, args.max_delay_minutes, args.seed)
    plan = summarize_plan(planned)
    hours = (planned[-1].send_time - planned[0].send_time).total_seconds() / 3600 if planned else 0
    print(
        f"Replaying {plan.events:,} events of {args.date} to {args.topic} at {args.speed:g}x "
        f"(about {hours * 3600 / args.speed / 60:.1f} minutes): {plan.late_events:,} late, "
        f"{plan.beyond_watermark:,} more than 30 minutes late"
    )
    sender = KafkaSender(args.bootstrap, args.topic)
    try:
        replay(planned, sender, args.speed)
    finally:
        sender.close()
    print(f"Sent {sender.sent:,} events.")
    return 0


def _run_stream(args: argparse.Namespace) -> int:
    from mobility_lakehouse.local_spark import create_local_spark
    from mobility_lakehouse.streaming import (
        kafka_events,
        local_streaming_paths,
        record_run,
        start_pickup_stream,
        summarize_run,
    )

    paths = local_streaming_paths(args.data_dir, args.lakehouse_dir)
    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC).replace(tzinfo=None)
    spark = create_local_spark("mobility-lakehouse-stream", kafka=True)
    # Keep the progress of every micro-batch of this run, to add up the late rows dropped.
    spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "100000")
    try:
        events = kafka_events(spark, args.bootstrap, args.topic, args.max_offsets_per_trigger)
        query = start_pickup_stream(events, paths, until_caught_up=args.until_caught_up)
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            query.stop()
        summary = summarize_run(spark, query, paths.table)
        record_run(spark, paths.runs, run_id, started_at, datetime.now(UTC).replace(tzinfo=None), summary)
    finally:
        spark.stop()

    print(
        f"Stream: {summary.batches} micro-batches, {summary.input_rows:,} events read, "
        f"{summary.dropped_late_rows:,} dropped as too late (watermark 30 minutes)\n"
        f"Table {paths.table}: {summary.windows:,} windows, {summary.trips:,} trips\n"
        f"Run {run_id} recorded in {paths.runs}"
    )
    return 0


def _run_reconcile(args: argparse.Namespace) -> int:
    from mobility_lakehouse.local_spark import create_local_spark
    from mobility_lakehouse.reconcile import (
        batch_hourly,
        compare,
        dropped_in_runs,
        render_report,
        stream_hourly,
        summary_lines,
    )
    from mobility_lakehouse.streaming import local_streaming_paths

    layout = local_layout(args.data_dir, args.lakehouse_dir)
    paths = local_streaming_paths(args.data_dir, args.lakehouse_dir)
    for folder, step in ((paths.table, "stream"), (paths.runs, "stream")):
        if not Path(folder).exists():
            print(f"Not found: {folder}\nRun first: mobility-lakehouse {step} --until-caught-up")
            return 1

    spark = create_local_spark("mobility-lakehouse-reconcile")
    try:
        batch = batch_hourly(spark, layout.gold.hourly_pickup_zones, args.date)
        stream = stream_hourly(spark, paths.table, args.date)
        dropped, runs = dropped_in_runs(spark, paths.runs)
    finally:
        spark.stop()

    if not batch:
        print(f"No batch gold for {args.date}. Run gold for {args.date:%Y-%m} first.")
        return 1
    result = compare(args.date, batch, stream, dropped, runs)
    args.report_dir.mkdir(parents=True, exist_ok=True)
    report = args.report_dir / f"streaming_reconciliation_{args.date}.md"
    report.write_text(render_report(result), encoding="utf-8")
    print("\n".join(summary_lines(result)))
    print(f"Report written to {report}")
    return 0 if result.reconciled else 1


def _run_stream_reset(args: argparse.Namespace) -> int:
    import shutil

    from mobility_lakehouse.replay import delete_topic
    from mobility_lakehouse.streaming import local_streaming_paths

    paths = local_streaming_paths(args.data_dir, args.lakehouse_dir)
    print(delete_topic(args.bootstrap, args.topic))
    for folder in (paths.table, paths.runs, paths.checkpoint):
        if Path(folder).exists():
            shutil.rmtree(folder)
            print(f"Deleted {folder}")
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
        "replay-extract": _run_replay_extract,
        "replay": _run_replay,
        "stream": _run_stream,
        "reconcile": _run_reconcile,
        "stream-reset": _run_stream_reset,
    }
    return commands[args.command](args)
