"""Replay planning and pacing (ADR 0012). Plain Python: no Kafka and no Spark needed."""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from mobility_lakehouse.replay import (
    WATERMARK_MINUTES,
    Event,
    plan_replay,
    replay,
    summarize_plan,
)
from mobility_lakehouse.streaming import WATERMARK_DELAY

START = datetime(2025, 1, 15, 8, 0)


def events(count: int) -> list[Event]:
    return [
        Event(f"e{n:05d}", "HV0003", 1 + n % 263, START + timedelta(seconds=30 * n)) for n in range(count)
    ]


def test_event_json_has_the_stream_schema() -> None:
    event = Event("2025-01-15-1", "HV0005", 132, datetime(2025, 1, 15, 8, 3, 0))

    assert json.loads(event.to_json()) == {
        "event_id": "2025-01-15-1",
        "hvfhs_license_num": "HV0005",
        "pickup_zone_id": 132,
        "event_time": "2025-01-15T08:03:00",
    }


def test_without_late_events_the_order_and_times_are_unchanged() -> None:
    planned = plan_replay(events(100), late_fraction=0.0)

    assert [p.event.event_id for p in planned] == [e.event_id for e in events(100)]
    assert all(p.send_time == p.event.event_time and p.delay_minutes == 0 for p in planned)


def test_late_events_are_sent_later_and_out_of_order() -> None:
    planned = plan_replay(events(2000), late_fraction=0.1, max_delay_minutes=90, seed=1)
    late = [p for p in planned if p.delay_minutes > 0]

    assert 150 < len(late) < 250  # about 10% of 2000
    assert all(1 <= p.delay_minutes <= 90 for p in late)
    assert all(p.send_time == p.event.event_time + timedelta(minutes=p.delay_minutes) for p in late)
    # Sent in send-time order, so event times are no longer in order.
    assert [p.send_time for p in planned] == sorted(p.send_time for p in planned)
    assert [p.event.event_time for p in planned] != sorted(p.event.event_time for p in planned)


def test_the_same_seed_gives_the_same_replay() -> None:
    first = plan_replay(events(500), late_fraction=0.2, seed=42)
    second = plan_replay(events(500), late_fraction=0.2, seed=42)
    other = plan_replay(events(500), late_fraction=0.2, seed=43)

    assert first == second
    assert first != other


def test_summary_counts_events_beyond_the_watermark() -> None:
    planned = plan_replay(events(3000), late_fraction=0.5, max_delay_minutes=90, seed=3)
    summary = summarize_plan(planned)

    assert summary.events == 3000
    assert summary.late_events == sum(1 for p in planned if p.delay_minutes > 0)
    assert summary.beyond_watermark == sum(1 for p in planned if p.delay_minutes > WATERMARK_MINUTES)
    assert 0 < summary.beyond_watermark < summary.late_events


@pytest.mark.parametrize(("fraction", "delay"), [(-0.1, 90), (1.5, 90), (0.1, 0.5)])
def test_invalid_plans_are_rejected(fraction: float, delay: float) -> None:
    with pytest.raises(ValueError):
        plan_replay(events(1), late_fraction=fraction, max_delay_minutes=delay)


def test_replay_waits_for_each_event_at_the_chosen_speed() -> None:
    planned = plan_replay(events(3), late_fraction=0.0)  # 30 seconds apart
    now = [0.0]
    sent: list[tuple[str, float]] = []

    def sleep(seconds: float) -> None:
        now[0] += seconds

    replay(planned, lambda e: sent.append((e.event_id, now[0])), speed=10, clock=lambda: now[0], sleep=sleep)

    # 30 seconds of event time at 10x speed is 3 seconds of waiting.
    assert sent == [("e00000", 0.0), ("e00001", 3.0), ("e00002", 6.0)]


def test_replay_needs_a_positive_speed() -> None:
    with pytest.raises(ValueError):
        replay(plan_replay(events(1)), lambda e: None, speed=0)


def test_replay_and_stream_use_the_same_watermark() -> None:
    assert f"{WATERMARK_MINUTES} minutes" == WATERMARK_DELAY
