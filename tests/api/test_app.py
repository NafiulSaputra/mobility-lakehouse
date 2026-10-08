"""HTTP behaviour of the API: status codes, validation and response shapes (ADR 0011)."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient  # noqa: E402

from mobility_lakehouse.api.app import create_app  # noqa: E402
from mobility_lakehouse.api.store import SnapshotStore  # noqa: E402


@pytest.fixture
def client(snapshot_dir: Path) -> TestClient:
    return TestClient(create_app(SnapshotStore(snapshot_dir)))


def test_health_lists_the_months(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "months": ["2025-02", "2025-03"]}


def test_driver_economics(client: TestClient) -> None:
    response = client.get("/drivers/economics", params={"month": "2025-03"})

    assert response.status_code == 200
    body = response.json()
    assert [row["company"] for row in body] == ["Uber", "Lyft"]
    assert body[0]["data_month"] == "2025-03-01"
    assert body[0]["driver_pay_per_mile"] == 4.0


def test_a_month_outside_the_snapshot_is_not_found(client: TestClient) -> None:
    response = client.get("/drivers/economics", params={"month": "2025-07"})

    assert response.status_code == 404
    assert "2025-02, 2025-03" in response.json()["detail"]


@pytest.mark.parametrize("month", ["2025-13", "march", "2025-3", ""])
def test_a_badly_written_month_is_rejected(client: TestClient, month: str) -> None:
    assert client.get("/drivers/economics", params={"month": month}).status_code == 422


def test_daily_trips(client: TestClient) -> None:
    params = {"start": "2025-03-01", "end": "2025-03-02", "company": "Uber"}
    response = client.get("/trips/daily", params=params)

    assert response.status_code == 200
    assert [row["trip_date"] for row in response.json()] == ["2025-03-01", "2025-03-02"]


@pytest.mark.parametrize(
    ("start", "end"),
    [("2025-03-02", "2025-03-01"), ("2025-01-01", "2025-06-30"), ("2025-03-01", "not-a-date")],
    ids=["end-before-start", "range-too-long", "bad-date"],
)
def test_daily_trips_rejects_bad_ranges(client: TestClient, start: str, end: str) -> None:
    assert client.get("/trips/daily", params={"start": start, "end": end}).status_code == 422


def test_busiest_zones(client: TestClient) -> None:
    response = client.get("/zones/busiest", params={"month": "2025-03", "hour": 18, "limit": 1})

    assert response.status_code == 200
    assert response.json() == [{"zone_id": 132, "borough": "Queens", "zone": "JFK Airport", "trips": 90}]


@pytest.mark.parametrize("params", [{"hour": 24}, {"hour": -1}, {"limit": 0}, {"limit": 51}])
def test_busiest_zones_rejects_values_out_of_range(client: TestClient, params: dict) -> None:
    assert client.get("/zones/busiest", params={"month": "2025-03", **params}).status_code == 422


def test_quality(client: TestClient) -> None:
    response = client.get("/quality/monthly")

    assert response.status_code == 200
    assert [row["spike_rules"] for row in response.json()] == [[], ["W003"]]


def test_openapi_documents_every_endpoint(client: TestClient) -> None:
    paths = client.get("/openapi.json").json()["paths"]

    assert set(paths) == {
        "/health",
        "/drivers/economics",
        "/trips/daily",
        "/zones/busiest",
        "/quality/monthly",
    }
