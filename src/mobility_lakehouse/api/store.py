"""Read-only access to the Parquet snapshot with DuckDB (ADR 0011).

Every snapshot table becomes a DuckDB view over its Parquet files, partitioned by ``data_month``. Queries use
parameters for every value that comes from a request, never string formatting.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from mobility_lakehouse.snapshot import SNAPSHOT_TABLES

# Same definition as the quality dashboard (ADR 0010): at least twice the previous month's rate, at least
# 100 matched rows.
SPIKE_FACTOR = 2
SPIKE_MIN_ROWS = 100


class SnapshotNotFoundError(RuntimeError):
    """The snapshot folder is missing a table, so the API cannot start."""


class SnapshotStore:
    """Queries over one snapshot folder. Safe to share between request threads."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._connection = duckdb.connect(database=":memory:")
        for name in SNAPSHOT_TABLES:
            folder = root / name
            if not any(folder.glob("*/*.parquet")):
                raise SnapshotNotFoundError(
                    f"no Parquet files in {folder}; export a snapshot first (see docs/runbook-databricks.md)"
                )
            pattern = (folder / "*" / "*.parquet").as_posix().replace("'", "''")
            self._connection.execute(
                f"CREATE VIEW {name} AS FROM read_parquet("
                f"'{pattern}', hive_partitioning = true, hive_types = {{'data_month': DATE}})"
            )

    def _rows(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        # A cursor is a separate connection to the same in-memory database, one per call, so concurrent
        # requests do not share state.
        cursor = self._connection.cursor()
        try:
            cursor.execute(sql, params or [])
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def months(self) -> list[date]:
        rows = self._rows("SELECT DISTINCT data_month FROM monthly_driver_economics ORDER BY data_month")
        return [row["data_month"] for row in rows]

    def driver_economics(self, month: date, company: str | None = None) -> list[dict[str, Any]]:
        where, params = ["data_month = ?"], [month]
        if company is not None:
            where.append("company = ?")
            params.append(company)
        return self._rows(
            f"""
            SELECT data_month, company, trips, driver_pay_total, trip_miles_total, trip_minutes_total,
                   driver_pay_per_mile, driver_pay_per_minute, driver_share_of_base_fare
            FROM monthly_driver_economics
            WHERE {" AND ".join(where)}
            ORDER BY trips DESC, company
            """,
            params,
        )

    def daily_trips(self, start: date, end: date, company: str | None = None) -> list[dict[str, Any]]:
        where, params = ["trip_date BETWEEN ? AND ?"], [start, end]
        if company is not None:
            where.append("company = ?")
            params.append(company)
        return self._rows(
            f"""
            SELECT trip_date, company, trips, base_fare_total, tips_total, driver_pay_total,
                   passenger_paid_total, avg_trip_miles, avg_trip_minutes
            FROM daily_company_trips
            WHERE {" AND ".join(where)}
            ORDER BY trip_date, company
            """,
            params,
        )

    def busiest_zones(self, month: date, hour: int | None = None, limit: int = 10) -> list[dict[str, Any]]:
        where, params = ["data_month = ?"], [month]
        if hour is not None:
            where.append("pickup_hour = ?")
            params.append(hour)
        return self._rows(
            f"""
            SELECT PULocationID AS zone_id, pickup_borough AS borough, pickup_zone AS zone,
                   SUM(trips) AS trips
            FROM hourly_pickup_zones
            WHERE {" AND ".join(where)}
            GROUP BY PULocationID, pickup_borough, pickup_zone
            ORDER BY trips DESC, zone_id
            LIMIT {int(limit)}
            """,
            params,
        )

    def quality(self) -> list[dict[str, Any]]:
        rows = self._rows(
            """
            WITH rates AS (
                SELECT data_month, rule_id, matched_rows,
                       matched_rows / NULLIF(total_rows, 0) AS match_rate
                FROM rule_counts
            ),
            flagged AS (
                SELECT data_month, rule_id,
                       matched_rows >= ?
                       AND match_rate >= ? * LAG(match_rate) OVER (PARTITION BY rule_id ORDER BY data_month)
                           AS is_spike
                FROM rates
            )
            SELECT q.data_month, q.rows_checked, q.rows_quarantined,
                   q.rows_quarantined / NULLIF(q.rows_checked, 0) AS quarantine_rate,
                   LIST(f.rule_id ORDER BY f.rule_id) FILTER (WHERE f.is_spike) AS spike_rules
            FROM quality_months AS q
            LEFT JOIN flagged AS f ON f.data_month = q.data_month
            GROUP BY q.data_month, q.rows_checked, q.rows_quarantined
            ORDER BY q.data_month
            """,
            [SPIKE_MIN_ROWS, SPIKE_FACTOR],
        )
        for row in rows:
            row["spike_rules"] = row["spike_rules"] or []
        return rows
