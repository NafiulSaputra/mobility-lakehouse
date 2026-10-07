# 0004. Quarantine rows that fail quality checks

- Status: Accepted
- Date: 2026-10-07
- Decider: Nafiul Hadi Saputra

## Context

The TLC does not guarantee the accuracy of the trip records. Some rows will contain impossible values, such as
negative fares or a drop-off time before the pickup time. These rows must not reach silver or gold.

Quality rules can also be wrong. A rule that is too strict removes valid trips, and nobody notices if the removed
rows are gone.

## Decision

Rows that fail at least one quality rule are written to a **quarantine** table instead of silver. Each quarantined
row keeps its original values plus the name of every rule it failed. Each run also records, per rule, how many rows
passed and failed.

The exact rule set is defined in Sprint 3, after profiling in Sprint 1.

## Consequences

- No row disappears silently. Every rejected row can be inspected and explained.
- If a rule turns out to be wrong, it can be fixed and the month reprocessed (see ADR 0003) without data loss.
- Per-rule metrics make a drop in data quality visible from one month to the next.
- One extra table to maintain, following the same monthly overwrite strategy.

## Alternatives considered

- **Drop failed rows and only count them.** Simpler, but failed rows cannot be inspected and a wrong rule cannot be
  detected. Rejected.
- **Keep failed rows in silver with a flag.** Every consumer would have to remember to filter them out. Rejected.
