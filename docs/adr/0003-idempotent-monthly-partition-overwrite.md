# 0003. Idempotent runs through monthly partition overwrite

- Status: Accepted, validated by profiling on 2026-10-07
- Date: 2026-10-07
- Decider: Nafiul Hadi Saputra

## Context

Pipelines fail and get rerun. The TLC can also update a month after it was published. Running the pipeline twice
for the same month must give exactly the same result, with no duplicated trips.

The source delivers one complete file per month. A correction replaces the whole month. The trip records are not
expected to contain a reliable unique trip identifier; this will be checked during profiling.

## Decision

Each run processes exactly one data month. In every layer it **replaces that month's partition** (Delta Lake
`replaceWhere` on the data month) instead of appending rows. The month is a job parameter.

## Consequences

- Rerunning a month is always safe: the second run replaces the first.
- Any old month can be reprocessed alone, without touching other months.
- No unique key is needed.
- A run rewrites the whole month even if only a few rows changed. With monthly files this is the natural unit
  of change, so the extra cost is small.
- An automated test must prove idempotency: run the same month twice and assert that row counts and contents are
  identical.

## Alternatives considered

- **Append.** Simple, but a rerun duplicates every row. Rejected.
- **MERGE on a key.** Efficient for small changes, but needs a unique key. Without a natural trip ID, a key built
  from column values could treat two real, identical-looking trips as one. Rejected unless profiling finds a
  reliable key.

## Validation

Profiling of two months ([2024-06](../profiling/fhvhv_2024-06.md) and [2025-01](../profiling/fhvhv_2025-01.md))
confirms the decision:

| Check | 2024-06 | 2025-01 |
|---|---|---|
| Rows | 20,123,226 | 20,405,666 |
| Identical full rows | 0 | 0 |
| Candidate key not unique (license, base, pickup, drop-off, pickup zone, drop-off zone) | 43 groups, 87 rows | 27 groups, 54 rows |
| Pickup time outside the file's month | 0 | 0 |

- The data dictionary has no trip identifier, and the most specific combination of columns is still not
  unique. A MERGE on that key would silently merge real, different trips.
- Every trip falls inside its file's month, so one file maps to exactly one month partition.
- Files contain no exact duplicates, so overwriting a month never needs de-duplication.

A guard against trips outside the partition month is still part of the quality rules (ADR 0006): it costs
nothing today and protects the overwrite strategy if a future file breaks the pattern.
