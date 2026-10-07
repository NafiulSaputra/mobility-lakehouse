# 0008. Gold tables: one table per business question

- Status: Accepted
- Date: 2026-10-07
- Decider: Nafiul Hadi Saputra

## Context

Silver holds about 20 million clean trips per month. Analysts, the dashboard (Sprint 5) and the API (Sprint 6)
need small, stable tables that answer specific questions without scanning every trip.

## Decision

Gold has one table per business question, each built from silver and partitioned by `data_month`:

| Table | Question | Grain |
|---|---|---|
| `daily_company_trips` | How many trips, how much fare, tips and driver pay per day per company? | day x company |
| `hourly_pickup_zones` | Which pickup zones are busiest, and at what hour? | day x hour x pickup zone |
| `monthly_driver_economics` | What do drivers earn per mile and per minute, and what share of the base fare? | month x company |

Rules that apply to every gold table:

- **Company names** come from the HVFHS licensee codes in the TLC data dictionary (HV0002 Juno, HV0003 Uber,
  HV0004 Via, HV0005 Lyft, listed "as of September 2019"). Unknown codes are kept as `Other (<code>)`, never
  dropped, so a new licensee shows up instead of disappearing.
- **Ratios are computed from totals** (total pay / total miles), not as an average of per-trip ratios. An average
  of ratios gives a short trip the same weight as a long one and overstates pay per mile.
- **A ratio with a zero denominator is null**, not zero or an error.
- **Passenger paid total** adds every charge the passenger pays: base fare, tolls, Black Car Fund, sales tax,
  congestion surcharge, airport fee, CBD congestion fee and tips. The CBD fee is null before 2025 (ADR 0006) and
  adds nothing for those months.
- **Gold only reads silver**, so quarantined rows (negative amounts, impossible times) are excluded from every
  metric. This is stated wherever these metrics are shown.
- Each run **rebuilds one month** of every gold table with the shared monthly overwrite (ADR 0003).

## Consequences

- Gold tables are small: one month of `daily_company_trips` has about (days x companies) rows, and
  `hourly_pickup_zones` at most (days x 24 x 265) rows.
- Day and hour come from the pickup time, which is New York local time (ADR 0006).
- Monthly figures across months can be compared directly because every month is built the same way.
- Adding a question means adding a table and a section to this ADR; existing tables do not change.

## Alternatives considered

- **One wide gold table with every metric.** Fewer tables, but every consumer must know which columns belong to
  which question, and the table grows with every new question. Rejected.
- **Pre-computed averages of per-trip ratios.** Simple to compute, but statistically misleading. Rejected.
- **Gold built from bronze with its own filters.** Would duplicate the quality rules. Rejected.
