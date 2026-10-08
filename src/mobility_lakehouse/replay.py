"""Replay one day of historical trips as a live event stream (ADR 0012).

The TLC publishes trips once a month, so there is no live feed. This module turns one day of silver trips into
events and sends them to Redpanda in the order they would have arrived, faster than real time:

- most events are sent at their pickup time;
- a chosen share is sent late, by a random delay, so the stream sees out-of-order and late data;
- the same seed always gives the same plan, so a replay can be repeated and checked.

The planning and pacing are plain Python and fully unit tested. Kafka and DuckDB are imported only when used.
"""

from __future__ import annotations

import json
import random
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

DEFAULT_TOPIC = "trips"
WATERMARK_MINUTES = 30  # must match streaming.WATERMARK_DELAY


@dataclass(frozen=True)
class Event:
    event_id: str
    hvfhs_license_num: str
    pickup_zone_id: int
    event_time: datetime  # pickup time, UTC

    def to_json(self) -> str:
        return json.dumps(
            {
                "event_id": self.event_id,
                "hvfhs_license_num": self.hvfhs_license_num,
                "pickup_zone_id": self.pickup_zone_id,
                "event_time": self.event_time.isoformat(timespec="seconds"),
            }
        )


@dataclass(frozen=True)
class PlannedEvent:
    send_time: datetime  # replay-clock time at which the event is sent
    delay_minutes: float
    event: Event


@dataclass(frozen=True)
class ReplayPlanSummary:
    events: int
    late_events: int  # sent after their pickup time
    beyond_watermark: int  # sent more than the watermark late: the stream is expected to drop most of them


def plan_replay(
    events: Iterable[Event],
    late_fraction: float = 0.02,
    max_delay_minutes: float = 90.0,
    seed: int = 7,
) -> list[PlannedEvent]:
    """Decide when each event is sent. Late events get a delay between 1 minute and ``max_delay_minutes``."""
    if not 0.0 <= late_fraction <= 1.0:
        raise ValueError("late_fraction must be between 0 and 1")
    if max_delay_minutes < 1.0:
        raise ValueError("max_delay_minutes must be at least 1")

    rng = random.Random(seed)
    planned = []
    for event in events:
        delay = rng.uniform(1.0, max_delay_minutes) if rng.random() < late_fraction else 0.0
        planned.append(PlannedEvent(event.event_time + timedelta(minutes=delay), delay, event))
    planned.sort(key=lambda p: (p.send_time, p.event.event_id))
    return planned


def summarize_plan(
    planned: Sequence[PlannedEvent], watermark_minutes: float = WATERMARK_MINUTES
) -> ReplayPlanSummary:
    return ReplayPlanSummary(
        events=len(planned),
        late_events=sum(1 for p in planned if p.delay_minutes > 0),
        beyond_watermark=sum(1 for p in planned if p.delay_minutes > watermark_minutes),
    )


def replay(
    planned: Sequence[PlannedEvent],
    send: Callable[[Event], None],
    speed: float,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Send events at their planned times, ``speed`` times faster than real time (600: 1 hour in 6 s)."""
    if speed <= 0:
        raise ValueError("speed must be positive")
    if not planned:
        return

    first = planned[0].send_time
    started = clock()
    for item in planned:
        due = (item.send_time - first).total_seconds() / speed
        wait = due - (clock() - started)
        if wait > 0.001:
            sleep(wait)
        send(item.event)


def read_events(events_dir: Path, day: date) -> list[Event]:
    """Read the events of one day written by ``mobility-lakehouse replay-extract``."""
    import duckdb

    folder = events_dir / f"event_date={day.isoformat()}"
    if not any(folder.glob("*.parquet")):
        raise FileNotFoundError(
            f"no events in {folder}; run first: mobility-lakehouse replay-extract --date {day}"
        )
    pattern = (folder / "*.parquet").as_posix().replace("'", "''")
    rows = duckdb.sql(
        "SELECT event_id, hvfhs_license_num, pickup_zone_id, event_time "
        f"FROM read_parquet('{pattern}') ORDER BY event_time, event_id"
    ).fetchall()
    return [
        Event(event_id, license_num, int(zone), event_time)
        for event_id, license_num, zone, event_time in rows
    ]


class KafkaSender:
    """Sends events to one topic. Idempotent producer: a retry never writes an event twice."""

    def __init__(self, bootstrap_servers: str, topic: str = DEFAULT_TOPIC) -> None:
        from confluent_kafka import Producer

        self.topic = topic
        self.sent = 0
        self._producer = Producer(
            {"bootstrap.servers": bootstrap_servers, "enable.idempotence": True, "linger.ms": 20}
        )

    def __call__(self, event: Event) -> None:
        while True:
            try:
                self._producer.produce(self.topic, key=str(event.pickup_zone_id), value=event.to_json())
                break
            except BufferError:
                # The local queue is full: let the producer deliver some messages, then try again.
                self._producer.poll(0.5)
        self.sent += 1
        if self.sent % 1000 == 0:
            self._producer.poll(0)

    def close(self) -> None:
        remaining = self._producer.flush(30)
        if remaining:
            raise RuntimeError(f"{remaining} events were not delivered to {self.topic}")


def delete_topic(bootstrap_servers: str, topic: str = DEFAULT_TOPIC) -> str:
    """Delete a topic so that the next replay starts from an empty stream. Returns what happened."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    if topic not in admin.list_topics(timeout=10).topics:
        return f"Topic {topic} does not exist"
    admin.delete_topics([topic], operation_timeout=30)[topic].result()
    return f"Deleted topic {topic}"
