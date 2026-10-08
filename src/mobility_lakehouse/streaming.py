"""Streaming pickups per zone and 15-minute window from Redpanda with Spark Structured Streaming (ADR 0012).

    replay-extract   one day of silver trips -> event files (Parquet)
    replay           event files -> Redpanda topic, with late and out-of-order events
    stream           Redpanda topic -> trips per pickup zone and 15-minute window (Delta)

Late data: the watermark is 30 minutes. An event up to 30 minutes behind the latest event time seen is still
counted; an older one is dropped, and Spark reports how many were dropped. Results are merged into the Delta
table by window and zone, so a micro-batch that runs again after a failure writes the same rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mobility_lakehouse.delta_io import PARTITION_COLUMN, Table

if TYPE_CHECKING:
    from datetime import date, datetime

    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

EVENT_SCHEMA = "event_id string, hvfhs_license_num string, pickup_zone_id int, event_time timestamp"
WINDOW_DURATION = "15 minutes"
WATERMARK_DELAY = "30 minutes"  # must match replay.WATERMARK_MINUTES
WINDOW_COLUMNS = ("window_start", "window_end", "pickup_zone_id", "trips")


@dataclass(frozen=True)
class StreamingPaths:
    events_dir: str  # replay input, partitioned by event_date
    table: str  # Delta table with trips per window and zone
    checkpoint: str  # Spark checkpoint: offsets read, watermark and aggregation state
    runs: str  # Delta table with one row per stream run: events read and late rows dropped


def local_streaming_paths(data_dir: Path, lakehouse_dir: Path, dataset: str = "fhvhv") -> StreamingPaths:
    return StreamingPaths(
        events_dir=(data_dir / "replay" / f"{dataset}_events").as_posix(),
        table=(lakehouse_dir / "streaming" / f"{dataset}_pickups_15min").as_posix(),
        checkpoint=(lakehouse_dir / "_checkpoints" / f"{dataset}_pickups_15min").as_posix(),
        runs=(lakehouse_dir / "streaming" / f"{dataset}_stream_runs").as_posix(),
    )


def extract_replay_day(spark: SparkSession, silver: Table, day: date, events_dir: str) -> int:
    """Write the silver trips that started on ``day`` as replay events. Safe to rerun for the same day."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from mobility_lakehouse.tlc import Month

    month_start = Month(day.year, day.month).start
    trips = (
        silver.read(spark)
        .where(F.col(PARTITION_COLUMN) == F.lit(month_start))
        .where(F.to_date("pickup_datetime") == F.lit(day))
    )
    # A stable order gives every trip the same event ID on every run.
    order = Window.orderBy(
        "pickup_datetime", "PULocationID", "DOLocationID", "hvfhs_license_num", "trip_miles"
    )
    events = trips.select(
        F.concat(F.lit(f"{day.isoformat()}-"), F.row_number().over(order).cast("string")).alias("event_id"),
        "hvfhs_license_num",
        F.col("PULocationID").cast("int").alias("pickup_zone_id"),
        F.col("pickup_datetime").alias("event_time"),
        F.lit(day).alias("event_date"),
    )
    (
        events.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .parquet(events_dir)
    )
    written = spark.read.parquet(events_dir).where(F.col("event_date") == F.lit(day))
    return written.count()


def kafka_events(
    spark: SparkSession, bootstrap_servers: str, topic: str, max_offsets_per_trigger: int = 50_000
) -> DataFrame:
    """Events read from the start of a Kafka topic, parsed from JSON."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        .load()
    )
    return parse_events(raw)


def parse_events(raw: DataFrame) -> DataFrame:
    """Kafka records (binary ``value``) -> one row per event with the columns of EVENT_SCHEMA."""
    from pyspark.sql import functions as F

    parsed = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
    return parsed.select("e.*").where(F.col("event_time").isNotNull() & F.col("pickup_zone_id").isNotNull())


def pickups_per_window(events: DataFrame) -> DataFrame:
    """Trips per pickup zone and 15-minute window, with a 30-minute watermark on the pickup time."""
    from pyspark.sql import functions as F

    return (
        events.withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(F.window("event_time", WINDOW_DURATION).alias("window"), "pickup_zone_id")
        .agg(F.count(F.lit(1)).alias("trips"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "pickup_zone_id",
            "trips",
        )
    )


def merge_windows(batch: DataFrame, table_path: str) -> None:
    """Upsert the windows of one micro-batch. Running the same batch twice gives the same table."""
    from delta.tables import DeltaTable

    spark = batch.sparkSession
    if not DeltaTable.isDeltaTable(spark, table_path):
        batch.select(*WINDOW_COLUMNS).limit(0).write.format("delta").save(table_path)
    (
        DeltaTable.forPath(spark, table_path)
        .alias("t")
        .merge(
            batch.alias("s"),
            "t.window_start = s.window_start AND t.pickup_zone_id = s.pickup_zone_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def start_pickup_stream(events: DataFrame, paths: StreamingPaths, until_caught_up: bool) -> StreamingQuery:
    """Start the query. ``until_caught_up`` processes what is available and stops; otherwise it runs live."""
    writer = (
        pickups_per_window(events)
        .writeStream.outputMode("update")
        .foreachBatch(lambda batch, _batch_id: merge_windows(batch, paths.table))
        .option("checkpointLocation", paths.checkpoint)
        .queryName("pickups_15min")
    )
    writer = (
        writer.trigger(availableNow=True) if until_caught_up else writer.trigger(processingTime="5 seconds")
    )
    return writer.start()


def _field(item: Any, name: str) -> Any:
    return item[name] if isinstance(item, dict) else getattr(item, name)


def dropped_by_watermark(progress: list[Any]) -> int:
    """Rows that arrived too late and were dropped, summed over the micro-batches of one run."""
    return sum(
        int(_field(operator, "numRowsDroppedByWatermark") or 0)
        for item in progress
        for operator in (_field(item, "stateOperators") or [])
    )


@dataclass(frozen=True)
class StreamSummary:
    batches: int
    input_rows: int
    dropped_late_rows: int
    windows: int
    trips: int


def summarize_run(spark: SparkSession, query: StreamingQuery, table_path: str) -> StreamSummary:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    progress = list(query.recentProgress)
    input_rows = sum(int(_field(item, "numInputRows") or 0) for item in progress)
    windows, trips = 0, 0
    if DeltaTable.isDeltaTable(spark, table_path):
        totals = (
            spark.read.format("delta")
            .load(table_path)
            .agg(F.count(F.lit(1)).alias("windows"), F.sum("trips").alias("trips"))
        )
        row = totals.first()
        windows, trips = int(row["windows"]), int(row["trips"] or 0)
    return StreamSummary(len(progress), input_rows, dropped_by_watermark(progress), windows, trips)


RUNS_SCHEMA = (
    "run_id string, started_at timestamp, finished_at timestamp, batches int, "
    "input_rows bigint, dropped_late_rows bigint"
)


def record_run(
    spark: SparkSession,
    runs_path: str,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    summary: StreamSummary,
) -> None:
    """Keep one row per stream run. Recording the same run again replaces its row."""
    from delta.tables import DeltaTable

    row = spark.createDataFrame(
        [(run_id, started_at, finished_at, summary.batches, summary.input_rows, summary.dropped_late_rows)],
        schema=RUNS_SCHEMA,
    )
    if not DeltaTable.isDeltaTable(spark, runs_path):
        row.limit(0).write.format("delta").save(runs_path)
    (
        DeltaTable.forPath(spark, runs_path)
        .alias("t")
        .merge(row.alias("s"), "t.run_id = s.run_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
