# 0001. Medallion architecture on Delta Lake

- Status: Accepted
- Date: 2026-10-07
- Decider: Nafiul Hadi Saputra

## Context

The source is monthly Parquet files from the NYC TLC. Files arrive about two months late, can be updated after
release, change schema over time, and come with no accuracy guarantee. Cleaning rules will change as the data is
better understood. When that happens, earlier results must be rebuilt without downloading the source again.

Writes must also be safe. A job that fails halfway must not leave half-written data visible to readers.

## Decision

Organize the data in three layers, all stored as Delta Lake tables:

- **Bronze**: source rows as delivered, plus load metadata (source file name, load time, data month).
- **Silver**: rows that passed the quality rules, with correct types and taxi zone names joined.
- **Gold**: aggregated, analysis-ready tables built from silver.

## Consequences

- If a cleaning rule changes, silver and gold can be rebuilt from bronze.
- Delta Lake provides ACID transactions, so each write is all or nothing.
- Delta Lake enforces the table schema, so unexpected columns or types fail loudly instead of corrupting a table.
- Table history (time travel) allows inspecting and restoring earlier versions.
- Storage is used three times. For this dataset size that cost is acceptable.

## Alternatives considered

- **Plain Parquet files.** No transactions, no schema enforcement, and no safe overwrite of one month. Rejected.
- **One cleaned table only.** Simpler, but a bug in the cleaning rules would require downloading the source again,
  and the original values would be lost. Rejected.
