# 0002. Scope v0.1.0 to HVFHV data from January 2024

- Status: Accepted
- Date: 2026-10-07
- Decider: Nafiul Hadi Saputra

## Context

The TLC publishes four trip datasets: yellow taxi, green taxi, For-Hire Vehicle (FHV) and High Volume For-Hire
Vehicle (HVFHV). Since 2019, trips by high-volume ride-hailing companies are reported in the separate, more detailed
HVFHV dataset.

The project runs on Databricks Free Edition, which is serverless-only with daily usage limits, and is developed on a
laptop with 12 GB of RAM. Processing every dataset and every year would use the quota without adding new
engineering problems.

## Decision

Version 0.1.0 processes only the **HVFHV** dataset, from **January 2024** to the latest month the TLC has published.

## Consequences

- The project matches its purpose: ride-hailing data.
- The range includes a real schema change: the `cbd_congestion_fee` column added for 2025 data onwards. The pipeline
  must handle months with and without this column.
- Workload stays within the Free Edition limits and local hardware.
- Other datasets and earlier years are out of scope. The design should not prevent adding them later.

## Alternatives considered

- **HVFHV plus yellow taxi.** More coverage, but roughly double the work and quota for the same engineering problems.
  Rejected for v0.1.0.
- **Only the latest three months.** Lighter, but it would miss the 2025 schema change. Rejected.
- **2019 onwards.** Most complete, but heavy for the available compute. Rejected for v0.1.0.
