"""Calendar and hour-of-day dimensions, without Spark (ADR 0014)."""

from __future__ import annotations

from datetime import date

import pytest

from mobility_lakehouse.dimensions import (
    CALENDAR_END,
    CALENDAR_START,
    DIM_DATE_DDL,
    DIM_TIME_DDL,
    PEAK_HOURS,
    check_in_calendar,
    date_key,
    date_rows,
    day_part,
    time_rows,
)
from mobility_lakehouse.tlc import Month

DATE_COLUMNS = [part.split()[0] for part in DIM_DATE_DDL.split(", ")]
TIME_COLUMNS = [part.split()[0] for part in DIM_TIME_DDL.split(", ")]


def row_for(day: date) -> dict:
    (row,) = [dict(zip(DATE_COLUMNS, r, strict=True)) for r in date_rows(day, day)]
    return row


def test_date_key_is_yyyymmdd() -> None:
    assert date_key(date(2025, 1, 15)) == 20250115
    assert date_key(date(2024, 12, 1)) == 20241201


def test_calendar_covers_the_project_scope_without_gaps() -> None:
    rows = date_rows()
    keys = [r[0] for r in rows]

    assert rows[0][1] == CALENDAR_START == date(2024, 1, 1)  # ADR 0002: January 2024 onwards
    assert rows[-1][1] == CALENDAR_END
    assert len(rows) == (CALENDAR_END - CALENDAR_START).days + 1
    assert keys == sorted(set(keys)), "keys must be unique and in date order"
    assert date(2024, 2, 29) in {r[1] for r in rows}  # leap day


def test_calendar_attributes() -> None:
    wednesday = row_for(date(2025, 1, 15))
    assert wednesday["date_key"] == 20250115
    assert (wednesday["year"], wednesday["quarter"], wednesday["month"]) == (2025, 1, 1)
    assert (wednesday["month_name"], wednesday["month_start"]) == ("January", date(2025, 1, 1))
    assert wednesday["day_of_month"] == 15
    assert (wednesday["day_of_week"], wednesday["day_name"]) == (3, "Wednesday")
    assert wednesday["is_weekend"] is False

    saturday = row_for(date(2025, 1, 18))
    sunday = row_for(date(2025, 1, 19))
    assert (saturday["day_name"], saturday["is_weekend"]) == ("Saturday", True)
    assert (sunday["day_of_week"], sunday["is_weekend"]) == (7, True)

    assert row_for(date(2025, 10, 1))["quarter"] == 4


def test_rows_match_the_table_schema() -> None:
    assert all(len(r) == len(DATE_COLUMNS) for r in date_rows(date(2025, 1, 1), date(2025, 1, 31)))
    assert all(len(r) == len(TIME_COLUMNS) for r in time_rows())


def test_calendar_end_before_start_is_rejected() -> None:
    with pytest.raises(ValueError, match="before its start"):
        date_rows(date(2025, 2, 1), date(2025, 1, 1))


@pytest.mark.parametrize("month", ["2024-01", "2025-06", "2027-12"])
def test_months_inside_the_calendar_are_accepted(month: str) -> None:
    check_in_calendar(Month.parse(month))


@pytest.mark.parametrize("month", ["2023-12", "2028-01"])
def test_months_outside_the_calendar_stop_the_run(month: str) -> None:
    with pytest.raises(ValueError, match="outside the calendar"):
        check_in_calendar(Month.parse(month))


def test_one_row_per_hour() -> None:
    rows = [dict(zip(TIME_COLUMNS, r, strict=True)) for r in time_rows()]

    assert [r["time_key"] for r in rows] == list(range(24))
    assert rows[8]["hour_label"] == "08:00"
    assert {r["hour"] for r in rows if r["is_peak_hour"]} == set(PEAK_HOURS)


def test_peak_hours_are_the_weekday_commute() -> None:
    assert {7, 8, 9, 16, 17, 18, 19} == PEAK_HOURS


@pytest.mark.parametrize(
    ("hour", "expected"),
    [
        (0, "Night"),
        (5, "Night"),
        (6, "Morning"),
        (11, "Morning"),
        (12, "Afternoon"),
        (17, "Afternoon"),
        (18, "Evening"),
        (23, "Evening"),
    ],
)
def test_day_parts(hour: int, expected: str) -> None:
    assert day_part(hour) == expected


@pytest.mark.parametrize("hour", [-1, 24])
def test_hour_outside_the_day_is_rejected(hour: int) -> None:
    with pytest.raises(ValueError, match="between 0 and 23"):
        day_part(hour)
