# 0011. Serve the API from a Parquet snapshot with DuckDB

- Status: Accepted
- Date: 2026-10-08
- Decider: Nafiul Hadi Saputra

## Context

Sprint 6 adds an HTTP API on top of the gold tables: driver economics, daily trips, busiest zones and the
data quality summary. The gold tables live in Unity Catalog on Databricks Free Edition, and two months also
exist locally in Docker.

Querying Databricks on every request has costs that do not fit this project:

- The Free Edition SQL warehouse has to start before the first query, which takes seconds to minutes.
- Every query uses the daily compute quota.
- The API would need a Databricks token, a secret to store and rotate.
- The API could not be tested in CI without a Databricks workspace.

The data the API serves is small: about 190,000 rows per month, mostly in `hourly_pickup_zones`.

## Decision

- **A snapshot step** (`snapshot`) copies one month of the gold tables, the rule counts and a per-month
  quality summary into Parquet folders partitioned by `data_month`. It is a fourth task of the monthly job and
  also a job of its own (`export_snapshot`) for months that are already in gold.
- **The snapshot is idempotent per month**: it uses Spark's dynamic partition overwrite, so writing a month
  again replaces only that month, like the Delta tables (ADR 0003).
- **On Databricks the snapshot is written to the volume `serving`.** It is downloaded with
  `databricks fs cp`. Locally, `mobility-lakehouse snapshot` writes it to `data/snapshot`.
- **The API reads the snapshot with DuckDB**, one read-only view per table. It does not depend on Spark,
  Java or Databricks, and the snapshot folder is mounted read-only into its container.
- **FastAPI** provides request validation and OpenAPI documentation (`/docs`). Every value from a request is
  passed to DuckDB as a query parameter, never formatted into SQL.
- **The API libraries are a separate dependency group** (`api`), so they are not installed on Databricks
  with the job wheel.
- **The spike definition is shared** with the quality dashboard (ADR 0010); a test checks that the API
  constants and the dashboard query agree.

## Consequences

- The API answers in milliseconds and costs nothing to run, and every endpoint is tested in CI, including a
  Spark test that writes a snapshot and reads it back through the API store.
- The API is only as fresh as the last downloaded snapshot. For monthly data published two months late, this
  is acceptable; the snapshot months are listed by `/health`.
- Downloading the snapshot is a manual step. The local folder must be deleted before a new download, because
  a rewritten month has new file names and old files would otherwise be read twice. The runbook says so.
- On Databricks, files from a failed write can stay in the volume. DuckDB would read them. A failed snapshot
  task must be rerun before downloading.

## Alternatives considered

- **Databricks SQL on every request.** Always fresh, but slow to start, uses quota, needs a token and cannot
  run in CI. Rejected for now; the store class is the single place to change if it is needed later.
- **Read the Delta tables directly** with a Delta reader. Avoids the copy, but Unity Catalog managed tables
  cannot be downloaded, and reader support for newer Delta table features varies. Rejected.
- **PostgreSQL loaded from gold.** A common serving pattern, but one more service to run and keep in sync,
  for read-only data that fits in a few Parquet files. Rejected.
