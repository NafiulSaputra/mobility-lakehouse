# Changelog

All notable changes to this project are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-10-08

First release: a batch lakehouse for NYC TLC High Volume For-Hire Vehicle trips, January 2024 onwards.

### Added

- Download of monthly trip files and the taxi zone lookup, safe to rerun.
- Bronze: source rows with load metadata, checked against an explicit schema contract (ADR 0007).
- Silver: 7 reject rules and 5 warning rules, a quarantine table with the failed rule IDs, and per-rule counts
  for every month (ADR 0004, 0006).
- Gold: daily trips per company, hourly pickups per zone, monthly driver economics (ADR 0008).
- Idempotent monthly partition overwrite for every table (ADR 0003).
- Local Spark and Delta Lake in Docker, pinned to Databricks serverless environment 5 (ADR 0005).
- Databricks Asset Bundle with a three-task serverless job and a runbook (ADR 0009).
- Data profiling reports for 2024-06 and 2025-01.
- CI on every pull request: lint, format, pipeline-lint, unit tests and Spark tests.

### Validated

- January 2025 gives identical counts locally and on Databricks Free Edition, and a rerun changes nothing.

[0.1.0]: https://github.com/NafiulSaputra/mobility-lakehouse/releases/tag/v0.1.0
