# 0003. Idempotent runs through monthly partition overwrite

- Status: Accepted, to be validated in Sprint 1 (data profiling)
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

If Sprint 1 profiling finds a reliable unique trip identifier, or finds that files contain many trips outside their
own month, this decision is reviewed and a new ADR is written.
