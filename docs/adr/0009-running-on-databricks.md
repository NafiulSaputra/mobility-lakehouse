# 0009. Running the pipeline on Databricks

- Status: Accepted, validated on Databricks Free Edition
- Date: 2026-10-07
- Decider: Nafiul Hadi Saputra

## Context

The pipeline runs locally in Docker (ADR 0005). It must also run on Databricks, the platform this project
targets, using Databricks Free Edition. The documented limits that shape the design:

- Free Edition is serverless only, with a daily quota; exceeding it shuts compute down for the rest of the day.
- Outbound internet access from Free Edition is restricted to a limited set of trusted domains, so a job
  cannot be assumed to reach the TLC download site.
- Serverless compute does not support DataFrame caching: `df.cache()`, `df.persist()` and `df.unpersist()` are
  not supported. Only Spark Connect APIs are supported.
- Serverless caches job environments per wheel version, so redeploying the same version can run old code.

## Decision

- **One pipeline, two layouts.** The steps (bronze, silver, gold) live in `pipeline.py` and are shared by the
  local CLI and the Databricks job. A `Layout` says where data lives: local folders, or Unity Catalog tables
  `<catalog>.<schema>.<layer>_<dataset>_<name>` and the volume `/Volumes/<catalog>/<schema>/raw`.
- **Raw files are uploaded to a Unity Catalog volume** with `databricks fs cp`, instead of being downloaded by
  the job. The download step stays local, where it is tested.
- **No caching on Databricks.** Silver persists its evaluated rows only locally. On Databricks it recomputes
  the rules for each of its three tables; the extra compute is accepted to keep one code path.
- **Databricks Asset Bundle** (`databricks.yml`) defines the job as code: one serverless environment (version 5,
  Python 3.12) with the project wheel, and three `python_wheel_task`s in order `bronze -> silver -> gold`,
  with the month as a job parameter. `dynamic_version` gives every deploy a new wheel version.
- **Schema and volume are created once with the CLI** (see the runbook), not by the bundle, to keep table and
  volume names independent of bundle deployment modes.
- A job task **fails loudly**: steps raise an error when their input is missing or a month comes out empty.

## Consequences

- Moving between local and Databricks changes configuration, not pipeline code.
- An end-to-end Spark test runs the shared steps with caching disabled, covering the Databricks code path in CI.
- Uploading about 0.5 GB per month from a laptop is a manual step. It is documented in the runbook and is the
  first thing to automate if the source becomes reachable from Databricks.
- Silver on Databricks costs more compute than locally because of the missing cache. If the daily quota
  becomes a problem, the first option is to stage evaluated rows in a temporary Delta table.

## Validation

Validated on 2026-10-07 with the January 2025 file on Databricks Free Edition (serverless, environment 5).
The first run of the three tasks took about 3.5 minutes including compute start-up. Every count matches the
local Docker run of the same month:

| Measure | Local (Docker) | Databricks |
|---|---|---|
| Bronze rows | 20,405,666 | 20,405,666 |
| Silver rows | 20,402,922 | 20,402,922 |
| Quarantined rows (Q003 4, Q004 2,427, Q005 313) | 2,744 | 2,744 |
| Rule count rows | 12 | 12 |
| Gold daily company rows | 62 | 62 |
| Uber / Lyft trips | 15,353,745 / 5,049,177 | 15,353,745 / 5,049,177 |

The job was then run a second time for the same month. All table counts stayed the same, so the monthly
overwrite (ADR 0003) is idempotent on Unity Catalog tables as well. The one fix needed was operational: the
volume folders must be created before the first upload, which is now in the runbook.

## Alternatives considered

- **Notebooks in the workspace.** Quick to start, but the code would fork from the tested package. Rejected.
- **Download inside the job.** Simplest pipeline, but depends on outbound access that Free Edition restricts.
  Rejected for now.
- **Bundle-managed schema and volume.** More infrastructure as code, but development mode can change resource
  names, which would also change the volume path the job reads. Rejected for v0.1.0.
