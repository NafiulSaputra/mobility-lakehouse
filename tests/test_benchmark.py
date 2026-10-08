"""Benchmark statistics and reports (ADR 0013). Plain Python: no Spark needed."""

from __future__ import annotations

from datetime import datetime

import pytest

from mobility_lakehouse.benchmark import (
    LocalBenchmark,
    StepTiming,
    format_seconds,
    parse_databricks_runs,
    render_databricks,
    render_local,
    stats,
    streaming_throughput,
)


def test_stats_and_formatting() -> None:
    assert stats([3.0, 1.0, 2.0]) == stats([2.0, 3.0, 1.0])
    s = stats([30.0, 10.0, 20.0, 100.0])
    assert (s.median, s.minimum, s.maximum, s.count) == (25.0, 10.0, 100.0, 4)
    assert format_seconds(42.04) == "42.0s"
    assert format_seconds(197) == "3m 17s"
    with pytest.raises(ValueError):
        stats([])


def local_result() -> LocalBenchmark:
    seconds = {
        "bronze": [60, 40, 41],
        "silver": [200, 150, 160],
        "gold": [30, 20, 22],
        "snapshot": [10, 8, 9],
    }
    timings = tuple(
        StepTiming(step, repeat + 1, float(values[repeat]))
        for repeat in range(3)
        for step, values in seconds.items()
    )
    return LocalBenchmark(
        month="2025-01",
        repeats=3,
        timings=timings,
        rows={"bronze": 20_405_666},
        environment={"Spark": "4.1.1"},
        measured_at="2026-10-08 10:00 UTC",
    )


def test_local_benchmark_statistics() -> None:
    result = local_result()

    assert result.step_stats("silver").median == 160.0
    total = result.total_stats()  # per repeat: 300, 218, 232
    assert (total.median, total.minimum, total.maximum) == (232.0, 218.0, 300.0)


def test_local_report() -> None:
    report = render_local(result=local_result())

    assert report.startswith("# Local benchmark: 2025-01")
    assert "| silver | 2m 40s | 2m 30s | 3m 20s |" in report
    assert "| **all four steps** | **3m 52s** | 3m 38s | 5m 00s |" in report
    assert "| bronze | 20,405,666 |" in report


def test_streaming_throughput_from_the_run_log() -> None:
    runs = [
        {
            "input_rows": 656_895,
            "started_at": datetime(2026, 10, 8, 9, 0),
            "finished_at": datetime(2026, 10, 8, 9, 2),
        },
        {
            "input_rows": 0,
            "started_at": datetime(2026, 10, 8, 9, 5),
            "finished_at": datetime(2026, 10, 8, 9, 6),
        },
    ]

    result = streaming_throughput(runs)

    assert (result.runs, result.events, result.seconds) == (1, 656_895, 120.0)
    assert round(result.events_per_second) == 5474
    assert streaming_throughput([]) is None


def run(run_id: int, month: str, ok: bool = True, **tasks: tuple[int, int]) -> dict:
    """A run as the Jobs API returns it; task times are milliseconds after the run start."""
    start = 1_791_400_000_000 + run_id * 1_000_000
    return {
        "run_id": run_id,
        "start_time": start,
        "end_time": start + 200_000,
        "state": {"result_state": "SUCCESS" if ok else "FAILED"},
        "job_parameters": [{"name": "month", "value": month}],
        "tasks": [
            {"task_key": key, "start_time": start + begin, "end_time": start + finish}
            for key, (begin, finish) in tasks.items()
        ],
    }


def test_databricks_runs_are_parsed_and_failed_runs_skipped() -> None:
    data = [
        run(1, "2025-01", bronze=(0, 60_000), silver=(60_000, 150_000)),
        run(2, "2025-02", ok=False, bronze=(0, 1000)),
        run(3, "2025-03", bronze=(0, 50_000), silver=(50_000, 130_000)),
    ]

    runs = parse_databricks_runs(data)

    assert [(r.run_id, r.month, r.seconds) for r in runs] == [(1, "2025-01", 200.0), (3, "2025-03", 200.0)]
    assert runs[0].tasks == {"bronze": 60.0, "silver": 90.0}
    assert parse_databricks_runs({"runs": data}) == runs


def test_task_time_falls_back_to_reported_durations() -> None:
    data = [run(1, "2025-01")]
    data[0]["tasks"] = [{"task_key": "gold", "setup_duration": 5000, "execution_duration": 25000}]

    assert parse_databricks_runs(data)[0].tasks == {"gold": 30.0}


def test_databricks_report() -> None:
    report = render_databricks(
        parse_databricks_runs([run(1, "2025-01", bronze=(0, 60_000)), run(2, "2025-02", bronze=(0, 40_000))])
    )

    assert report.startswith("# Databricks benchmark")
    assert "| bronze | 2 | 50.0s | 40.0s | 1m 00s |" in report
    assert "| **whole job** | 2 | **3m 20s** |" in report
    with pytest.raises(ValueError):
        render_databricks([])
