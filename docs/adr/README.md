# Architecture Decision Records

Each file records one significant decision: the context, the decision, its consequences and the alternatives that
were considered. Records are never deleted. When a decision changes, a new ADR supersedes the old one and the old
one is marked `Superseded`.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-medallion-architecture-with-delta-lake.md) | Medallion architecture on Delta Lake | Accepted |
| [0002](0002-scope-hvfhv-2024-onwards.md) | Scope v0.1.0 to HVFHV data from January 2024 | Accepted |
| [0003](0003-idempotent-monthly-partition-overwrite.md) | Idempotent runs through monthly partition overwrite | Accepted, validated by profiling |
| [0004](0004-quarantine-failed-rows.md) | Quarantine rows that fail quality checks | Accepted |
| [0005](0005-local-spark-in-docker-pinned-to-databricks.md) | Local Spark in Docker, pinned to Databricks serverless versions | Accepted |
| [0006](0006-data-quality-rules-and-silver-schema.md) | Data quality rules and silver schema | Accepted |
| [0007](0007-explicit-bronze-schema-contract.md) | Explicit schema contract for bronze | Accepted |
| [0008](0008-gold-tables.md) | Gold tables: one table per business question | Accepted |

## Template

```markdown
# NNNN. Title

- Status: Proposed | Accepted | Superseded by NNNN
- Date: YYYY-MM-DD
- Decider: Nafiul Hadi Saputra

## Context
## Decision
## Consequences
## Alternatives considered
```
