"""Response models. They are the API contract and appear in the OpenAPI documentation at /docs."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field


class Health(BaseModel):
    status: str = Field(examples=["ok"])
    months: list[str] = Field(
        description="Months in the snapshot, YYYY-MM", examples=[["2025-01", "2025-02"]]
    )


class DriverEconomics(BaseModel):
    data_month: date
    company: str = Field(examples=["Uber"])
    trips: int
    driver_pay_total: float
    trip_miles_total: float
    trip_minutes_total: float
    driver_pay_per_mile: float | None = Field(description="Total pay / total miles; null when miles are 0")
    driver_pay_per_minute: float | None = Field(
        description="Total pay / total minutes; null when minutes are 0"
    )
    driver_share_of_base_fare: float | None = Field(description="Total pay / total base fare")


class DailyTrips(BaseModel):
    trip_date: date
    company: str
    trips: int
    base_fare_total: float
    tips_total: float
    driver_pay_total: float
    passenger_paid_total: float
    avg_trip_miles: float | None
    avg_trip_minutes: float | None


class Zone(BaseModel):
    zone_id: int = Field(description="TLC taxi zone ID (PULocationID)")
    borough: str | None
    zone: str | None
    trips: int


class QualityMonth(BaseModel):
    data_month: date
    rows_checked: int
    rows_quarantined: int
    quarantine_rate: float | None
    spike_rules: list[str] = Field(
        description="Rules whose match rate is at least twice the previous month's, with at least 100 rows",
        examples=[["W003"]],
    )
