# 0006. Data quality rules and silver schema

- Status: Accepted
- Date: 2026-10-07
- Decider: Nafiul Hadi Saputra

## Context

ADR 0004 decided that rows failing quality checks go to a quarantine table, but left the rules open until the
data was profiled. Two months were profiled: [2024-06](../profiling/fhvhv_2024-06.md) (20,123,226 rows) and
[2025-01](../profiling/fhvhv_2025-01.md) (20,405,666 rows).

Not every unusual value is an error. Some patterns are too frequent and too stable to be random corruption, and
quarantining them would remove real trips from analysis. Rules therefore have two severities.

## Decision

### Reject rules: the row goes to quarantine

| ID | Rule | Condition | 2024-06 | 2025-01 |
|---|---|---|---|---|
| Q001 | Required field missing | Null in license, base, pickup, drop-off, pickup zone, drop-off zone, base fare or driver pay | 0 | 0 |
| Q002 | Pickup outside partition month | `pickup_datetime` not in the month being loaded | 0 | 0 |
| Q003 | Drop-off not after pickup | `dropoff_datetime <= pickup_datetime` | 3 | 4 |
| Q004 | Negative base fare | `base_passenger_fare < 0` | 199 | 2,427 |
| Q005 | Negative driver pay | `driver_pay < 0` | 33 | 313 |
| Q006 | Negative distance or duration | `trip_miles < 0` or `trip_time < 0` | 0 | 0 |
| Q007 | Zone ID out of range | Pickup or drop-off zone outside 1-265 | 0 | 0 |

Negative amounts may be refunds or adjustments rather than trips. They are quarantined so that revenue and pay
metrics in gold only describe completed trips, and they stay available for inspection.

### Warning rules: the row is kept and counted

| ID | Rule | Condition | 2024-06 | 2025-01 |
|---|---|---|---|---|
| W001 | Pickup before request | `pickup_datetime < request_datetime` | 183,600 (0.91%) | 199,788 (0.98%) |
| W002 | Zero distance | `trip_miles = 0` | 2,655 | 2,606 |
| W003 | Zero base fare | `base_passenger_fare = 0` | 169 | 14,777 |
| W004 | Very long trip | `trip_time > 21600` (6 hours) | 65 | 18 |
| W005 | Candidate key collision | Same license, base, pickup, drop-off and zones as another row | 43 groups | 27 groups |

W001 affects about 1% of trips in both months. A stable pattern at that rate is more likely a property of how
times are reported than random corruption, so these trips stay in silver. The W004 threshold is a first choice
(the 99th percentile of `trip_time` is about one hour); it only counts rows and can be tuned without data loss.

Every run records, per month and per rule, how many rows matched. The silver implementation (Sprint 3)
reproduced every count measured during profiling exactly; W004 was first measured there. A sudden change in these counts is the
signal that the source changed.

### Silver schema decisions

- **`cbd_congestion_fee`** exists in silver for every month. For files that do not have the column (before
  2025) the value is **null**, not 0: the fee did not exist yet, and 0 would claim that a fee of zero was charged.
- **Timestamps** stay `timestamp_ntz` (no time zone), as delivered. They are New York local time. Converting them
  would require choosing a time zone rule for daylight saving gaps without any benefit for monthly analysis.
- **`originating_base_num` and `on_scene_datetime`** are nullable by design (about 25% null in both months). Nulls
  in these columns are not quality failures.

## Consequences

- About 0.001% (2024-06) to 0.013% (2025-01) of rows are quarantined, mostly negative amounts.
- Gold revenue and pay metrics exclude negative adjustments. This must be stated wherever those metrics are shown.
- Rule IDs are stable, so quarantine rows, metrics and documentation can refer to the same rule.
- Rules are based on two months. New months may reveal new patterns; rules are reviewed when the per-rule counts
  change sharply.

## Alternatives considered

- **Keep negative amounts in silver with a flag.** Complete data, but every consumer must remember to filter.
  Rejected.
- **Quarantine pickup-before-request rows.** Would remove about 1% of real trips every month. Rejected.
- **Fill the missing congestion fee with 0.** Simpler queries, but it misrepresents trips before the fee existed.
  Rejected.
