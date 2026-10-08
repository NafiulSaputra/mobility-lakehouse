"""FastAPI application over the Parquet snapshot (ADR 0011).

    uvicorn --factory mobility_lakehouse.api.app:create_app      # snapshot from $MOBILITY_SNAPSHOT_DIR
    docker compose up --build api                                # same, in Docker, on http://localhost:8000

Interactive documentation: http://localhost:8000/docs
"""

import os
from datetime import date
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query

from mobility_lakehouse.api.models import DailyTrips, DriverEconomics, Health, QualityMonth, Zone
from mobility_lakehouse.api.store import SnapshotStore
from mobility_lakehouse.tlc import Month

SNAPSHOT_DIR_ENV = "MOBILITY_SNAPSHOT_DIR"
MAX_DAYS = 92  # about one quarter: keeps daily responses small


def _version() -> str:
    try:
        return version("mobility-lakehouse")
    except PackageNotFoundError:
        return "unknown"


MonthParam = Annotated[
    str,
    Query(pattern=r"^\d{4}-(0[1-9]|1[0-2])$", description="Data month, YYYY-MM", examples=["2025-03"]),
]
CompanyParam = Annotated[
    str | None, Query(description="Company name as in gold, for example Uber or Lyft", examples=["Uber"])
]


def create_app(store: SnapshotStore | None = None) -> FastAPI:
    """Build the app. Without a store, the snapshot folder comes from $MOBILITY_SNAPSHOT_DIR."""
    if store is None:
        store = SnapshotStore(Path(os.environ.get(SNAPSHOT_DIR_ENV, "data/snapshot")))

    app = FastAPI(
        title="mobility-lakehouse API",
        version=_version(),
        description="Gold metrics and data quality for NYC ride-hailing trips, from a Parquet snapshot.",
    )

    def known_month(text: str) -> date:
        month = Month.parse(text).start
        if month not in store.months():
            available = ", ".join(m.strftime("%Y-%m") for m in store.months())
            raise HTTPException(status_code=404, detail=f"month {text} is not in the snapshot ({available})")
        return month

    @app.get("/health", tags=["service"])
    def health() -> Health:
        return Health(status="ok", months=[m.strftime("%Y-%m") for m in store.months()])

    @app.get("/drivers/economics", tags=["gold"])
    def driver_economics(month: MonthParam, company: CompanyParam = None) -> list[DriverEconomics]:
        """What drivers earn per mile and per minute, per company, for one month."""
        return [DriverEconomics(**row) for row in store.driver_economics(known_month(month), company)]

    @app.get("/trips/daily", tags=["gold"])
    def daily_trips(
        start: Annotated[date, Query(description="First day, YYYY-MM-DD", examples=["2025-03-01"])],
        end: Annotated[date, Query(description="Last day, YYYY-MM-DD", examples=["2025-03-31"])],
        company: CompanyParam = None,
    ) -> list[DailyTrips]:
        """Trips, fares, tips and driver pay per day and company."""
        if end < start:
            raise HTTPException(status_code=422, detail="end must not be before start")
        if (end - start).days >= MAX_DAYS:
            raise HTTPException(status_code=422, detail=f"ask for at most {MAX_DAYS} days at a time")
        return [DailyTrips(**row) for row in store.daily_trips(start, end, company)]

    @app.get("/zones/busiest", tags=["gold"])
    def busiest_zones(
        month: MonthParam,
        hour: Annotated[int | None, Query(ge=0, le=23, description="Pickup hour, 0 to 23")] = None,
        limit: Annotated[int, Query(ge=1, le=50)] = 10,
    ) -> list[Zone]:
        """Pickup zones with the most trips in one month, optionally at one hour of the day."""
        return [Zone(**row) for row in store.busiest_zones(known_month(month), hour, limit)]

    @app.get("/quality/monthly", tags=["quality"])
    def quality() -> list[QualityMonth]:
        """Rows checked and quarantined per month, and the rules that spiked (same definition as ADR 0010)."""
        return [QualityMonth(**row) for row in store.quality()]

    return app
