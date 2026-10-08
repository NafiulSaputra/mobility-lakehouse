"""Streaming pickups per window with a 30-minute watermark (ADR 0012).

The tests feed events from JSON files instead of Redpanda: the query is the same, only the source differs.
Each run processes the new files and stops, like ``mobility-lakehouse stream --until-caught-up``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from mobility_lakehouse.streaming import (  # noqa: E402
    EVENT_SCHEMA,
    StreamingPaths,
    dropped_by_watermark,
    parse_events,
    start_pickup_stream,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

pytestmark = pytest.mark.spark


def write_events(folder: Path, name: str, events: list[tuple[int, str]]) -> None:
    """One JSON file of events: (pickup zone, time on 2025-01-15)."""
    folder.mkdir(parents=True, exist_ok=True)
    lines = [
        json.dumps(
            {
                "event_id": f"{name}-{n}",
                "hvfhs_license_num": "HV0003",
                "pickup_zone_id": zone,
                "event_time": f"2025-01-15T{time}:00",
            }
        )
        for n, (zone, time) in enumerate(events)
    ]
    (folder / f"{name}.json").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_until_caught_up(spark: SparkSession, source: Path, paths: StreamingPaths) -> int:
    """Process the files not seen yet and return the number of rows dropped by the watermark."""
    events = spark.readStream.schema(EVENT_SCHEMA).json(str(source))
    query = start_pickup_stream(events, paths, until_caught_up=True)
    query.awaitTermination()
    return dropped_by_watermark(list(query.recentProgress))


def windows(spark: SparkSession, paths: StreamingPaths) -> dict[tuple[str, int], int]:
    from pyspark.sql import functions as F

    rows = (
        spark.read.format("delta")
        .load(paths.table)
        .select(F.date_format("window_start", "HH:mm").alias("start"), "pickup_zone_id", "trips")
        .limit(1000)
        .collect()
    )
    return {(row["start"], row["pickup_zone_id"]): row["trips"] for row in rows}


@pytest.fixture
def setup(tmp_path: Path) -> tuple[Path, StreamingPaths]:
    paths = StreamingPaths(
        events_dir=str(tmp_path / "unused"),
        table=str(tmp_path / "pickups_15min"),
        checkpoint=str(tmp_path / "checkpoint"),
    )
    return tmp_path / "events", paths


def test_late_events_within_the_watermark_count_and_older_ones_are_dropped(
    spark: SparkSession, setup: tuple[Path, StreamingPaths]
) -> None:
    source, paths = setup

    # First run: the latest event is 09:00, so afterwards the watermark is 08:30.
    write_events(
        source, "001", [(1, "08:00"), (1, "08:05"), (1, "08:20"), (1, "08:40"), (1, "09:00"), (2, "08:10")]
    )
    assert run_until_caught_up(spark, source, paths) == 0
    assert windows(spark, paths) == {
        ("08:00", 1): 2,
        ("08:15", 1): 1,
        ("08:30", 1): 1,
        ("09:00", 1): 1,
        ("08:00", 2): 1,
    }

    # Second run: 08:10 is older than the watermark and is dropped; 08:35 is late but still counted.
    write_events(source, "002", [(1, "08:10"), (1, "08:35"), (1, "09:10")])
    assert run_until_caught_up(spark, source, paths) == 1
    after_late_data = windows(spark, paths)
    assert after_late_data[("08:00", 1)] == 2  # unchanged: the late 08:10 event was dropped
    assert after_late_data[("08:30", 1)] == 2  # 08:40 + the late 08:35
    assert after_late_data[("09:00", 1)] == 2  # 09:00 + 09:10

    # Third run, no new events: nothing changes.
    assert run_until_caught_up(spark, source, paths) == 0
    assert windows(spark, paths) == after_late_data


def test_kafka_values_are_parsed_and_broken_messages_skipped(spark: SparkSession) -> None:
    good = json.dumps(
        {
            "event_id": "a",
            "hvfhs_license_num": "HV0005",
            "pickup_zone_id": 132,
            "event_time": "2025-01-15T08:03:00",
        }
    )
    raw = spark.createDataFrame([(good,), ("not json",)], "value string")

    rows = parse_events(raw).limit(10).collect()

    assert len(rows) == 1
    assert (rows[0]["event_id"], rows[0]["pickup_zone_id"]) == ("a", 132)
