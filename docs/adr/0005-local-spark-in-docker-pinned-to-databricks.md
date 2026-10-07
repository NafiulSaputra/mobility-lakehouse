# 0005. Local Spark in Docker, pinned to Databricks serverless versions

- Status: Accepted
- Date: 2026-10-07
- Decider: Nafiul Hadi Saputra

## Context

Pipeline code must be developed and tested before it runs on Databricks. Databricks Free Edition has daily
usage limits, so every experiment there costs quota. Code that passes locally must also behave the same on
Databricks, which means the local Python, Spark, Delta Lake and Java versions must match it closely.

At the time of writing, Databricks serverless environment version 5 uses Python 3.12.3. Databricks
Runtime 18.0 includes Apache Spark 4.1.0 and uses JDK 21 by default. Delta Lake 4.3.x supports Apache
Spark 4.0.x and 4.1.x.

The development machine runs Windows. Running Spark directly on Windows needs extra setup and behaves
differently from the Linux machines used by CI and Databricks.

## Decision

Run Spark locally in a Docker container (`docker/spark/Dockerfile`, started with `compose.yaml`):

| Component | Version | Reason |
|---|---|---|
| Python | 3.12 | Serverless environment version 5 |
| PySpark | 4.1.1 | Spark 4.1, the Spark line of Databricks Runtime 18. Newest 4.1 patch allowed by delta-spark 4.3.1, which requires `pyspark>=4.0.1,<=4.1.1` |
| delta-spark | 4.3.1 | Supports Spark 4.1.x |
| Java | 21 | Default JDK of Databricks Runtime 18 |

PySpark and delta-spark are in a separate `spark` dependency group. They are needed locally but not on
Databricks, where the runtime provides Spark. Code that does not need Spark (downloads, report rendering)
does not import it, so it runs and is tested anywhere.

The Delta Lake jars are downloaded when the image is built, so containers start quickly and work offline.

## Consequences

- Local runs cost no Databricks quota and behave like Linux CI and Databricks.
- The container shares the repository folder, so data and reports land on the host drive.
- Databricks is a managed runtime. Its Spark and Delta Lake contain features and fixes not present in the
  open-source releases, so local runs are close to Databricks but not identical. Final verification
  happens on Databricks (Sprint 4).
- When Databricks moves to a new environment version, this ADR and the pinned versions must be reviewed.
- PySpark cannot be upgraded on its own: delta-spark pins the PySpark versions it supports, and `uv.lock`
  enforces that. Upgrades happen as a pair.

## Alternatives considered

- **Develop directly on Databricks.** Most realistic, but every test run uses the daily quota and is slow.
  Rejected as the main loop; used for final verification.
- **Spark installed directly on Windows.** Needs extra native setup and differs from Linux. Rejected.
- **Databricks Connect from the laptop.** Runs local code against Databricks compute, so it still uses
  the quota. May be added later for verification.
