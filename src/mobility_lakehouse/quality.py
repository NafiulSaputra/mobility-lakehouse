"""Data quality rules from ADR 0006, as data.

A rule's condition is a Spark SQL expression that is TRUE when a row VIOLATES the rule. Reject rules send
the row to quarantine; warning rules keep it in silver and are only counted. Rules are plain Python, so the
rule set can be tested and compared with ADR 0006 without Spark.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Severity(StrEnum):
    REJECT = "reject"
    WARNING = "warning"


@dataclass(frozen=True)
class Rule:
    id: str
    severity: Severity
    description: str
    condition: str  # Spark SQL, true when the row violates the rule


REQUIRED_COLUMNS = (
    "hvfhs_license_num",
    "dispatching_base_num",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "base_passenger_fare",
    "driver_pay",
)
MAX_ZONE_ID = 265
VERY_LONG_TRIP_SECONDS = 6 * 60 * 60
# Computed before the rules run: how many rows of the month share this row's candidate key.
CANDIDATE_KEY_ROWS = "_candidate_key_rows"

RULES: tuple[Rule, ...] = (
    Rule(
        "Q001",
        Severity.REJECT,
        "Required field missing",
        " OR ".join(f"{c} IS NULL" for c in REQUIRED_COLUMNS),
    ),
    Rule(
        "Q002",
        Severity.REJECT,
        "Pickup outside partition month",
        "pickup_datetime < data_month OR pickup_datetime >= add_months(data_month, 1)",
    ),
    Rule("Q003", Severity.REJECT, "Drop-off not after pickup", "dropoff_datetime <= pickup_datetime"),
    Rule("Q004", Severity.REJECT, "Negative base fare", "base_passenger_fare < 0"),
    Rule("Q005", Severity.REJECT, "Negative driver pay", "driver_pay < 0"),
    Rule("Q006", Severity.REJECT, "Negative distance or duration", "trip_miles < 0 OR trip_time < 0"),
    Rule(
        "Q007",
        Severity.REJECT,
        "Zone ID out of range",
        f"PULocationID NOT BETWEEN 1 AND {MAX_ZONE_ID} OR DOLocationID NOT BETWEEN 1 AND {MAX_ZONE_ID}",
    ),
    Rule("W001", Severity.WARNING, "Pickup before request", "pickup_datetime < request_datetime"),
    Rule("W002", Severity.WARNING, "Zero distance", "trip_miles = 0"),
    Rule("W003", Severity.WARNING, "Zero base fare", "base_passenger_fare = 0"),
    Rule("W004", Severity.WARNING, "Very long trip (over 6 hours)", f"trip_time > {VERY_LONG_TRIP_SECONDS}"),
    Rule("W005", Severity.WARNING, "Candidate key collision", f"{CANDIDATE_KEY_ROWS} > 1"),
)

CANDIDATE_KEY = (
    "hvfhs_license_num",
    "dispatching_base_num",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
)


def rules_of(severity: Severity) -> tuple[Rule, ...]:
    return tuple(r for r in RULES if r.severity is severity)
