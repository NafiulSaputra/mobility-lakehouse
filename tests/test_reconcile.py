"""Stream vs batch reconciliation logic (ADR 0012). Plain Python: no Spark needed."""

from __future__ import annotations

from datetime import date

from mobility_lakehouse.reconcile import compare, render_report, summary_lines

DAY = date(2025, 1, 15)
BATCH = {(8, 132): 100, (8, 161): 50, (9, 132): 70}


def test_a_stream_short_by_exactly_the_dropped_events_reconciles() -> None:
    stream = {(8, 132): 97, (8, 161): 50, (9, 132): 69}

    result = compare(DAY, BATCH, stream, dropped_late=4, runs=1)

    assert (result.batch_trips, result.stream_trips, result.missing) == (220, 216, 4)
    assert (result.cells, result.cells_equal, result.cells_short, result.cells_over) == (3, 1, 2, 0)
    assert result.reconciled


def test_missing_trips_that_spark_did_not_report_do_not_reconcile() -> None:
    stream = {(8, 132): 90, (8, 161): 50, (9, 132): 70}

    assert not compare(DAY, BATCH, stream, dropped_late=4, runs=1).reconciled


def test_a_stream_with_more_trips_than_batch_never_reconciles() -> None:
    # The totals match, but one zone-hour has trips that batch does not have.
    stream = {(8, 132): 96, (8, 161): 50, (9, 132): 70, (10, 1): 0, (9, 161): 0}
    stream[(8, 161)] = 51

    result = compare(DAY, BATCH, stream, dropped_late=3, runs=1)

    assert result.missing == 3
    assert result.cells_over == 1
    assert not result.reconciled


def test_zone_hours_missing_on_one_side_count_as_differences() -> None:
    result = compare(DAY, BATCH, {(8, 132): 100, (8, 161): 50}, dropped_late=70, runs=2)

    assert (result.cells, result.cells_short) == (3, 1)
    assert result.reconciled


def test_summary_and_report_state_the_verdict() -> None:
    result = compare(DAY, BATCH, {(8, 132): 99, (8, 161): 50, (9, 132): 70}, dropped_late=1, runs=1)

    assert summary_lines(result)[0] == "Reconciliation 2025-01-15: RECONCILED"
    report = render_report(result)
    assert report.startswith("# Streaming reconciliation: 2025-01-15")
    assert "**Result: reconciled.**" in report
    assert "| Dropped as too late, reported by Spark | 1 |" in report
