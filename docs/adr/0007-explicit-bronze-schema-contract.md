# 0007. Explicit schema contract for bronze

- Status: Accepted
- Date: 2026-10-07
- Decider: Nafiul Hadi Saputra

## Context

Bronze stores all months in one Delta table and replaces one month per run (ADR 0003). Months do not all have
the same columns: `cbd_congestion_fee` exists from 2025 onwards and is absent in 2024 files (profiling reports).
The TLC also warns that it may standardize the Parquet schema across years.

Delta Lake can add new columns automatically (`mergeSchema`). Neither the Delta Lake nor the Databricks
documentation describes whether this is supported together with `replaceWhere`, the option this project uses
for idempotent monthly overwrites. Databricks documents that `overwriteSchema` cannot be used with dynamic
partition overwrite.

## Decision

Bronze follows an **explicit schema contract**: the 25 published HVFHV columns and their types, defined in
`src/mobility_lakehouse/contracts.py`. Before writing, every file is conformed to the contract:

- Contract columns missing from the file are added as typed nulls (for example `cbd_congestion_fee` before 2025).
- Columns are cast to the contract type. Spark 4 runs in ANSI mode, so a value that cannot be cast fails the load
  instead of silently becoming null.
- **A column that is not in the contract stops the load** with a clear error. A new source column is a change to
  review: it is added to the contract on purpose, with a note in this ADR or a new one.

Bronze adds three metadata columns: `_source_file`, `_ingested_at` and `data_month` (the partition column).

## Consequences

- The bronze schema is predictable and documented in code. Every month has the same columns and types.
- Writes use only documented Delta features (`replaceWhere` without schema evolution).
- When the TLC adds a column, the pipeline stops until a person updates the contract. This is deliberate:
  no source change reaches the lakehouse unnoticed. The cost is a manual step for every new column.
- Idempotency compares data columns. `_ingested_at` records when a month was loaded, so it changes on a rerun
  by design.

## Alternatives considered

- **Automatic schema evolution (`mergeSchema`).** No manual step, but relies on behaviour that is not
  documented together with `replaceWhere`, and new columns would appear without review. Rejected.
- **Explicit contract that drops unknown columns.** The load never stops, but new source columns would be lost
  without anyone noticing. Rejected.
