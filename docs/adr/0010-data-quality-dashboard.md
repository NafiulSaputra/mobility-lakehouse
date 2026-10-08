# 0010. Data quality dashboard on Databricks AI/BI

- Status: Accepted
- Date: 2026-10-08
- Decider: Nafiul Hadi Saputra

## Context

Every silver run stores how many rows matched each quality rule (ADR 0006) and keeps every rejected row in
quarantine (ADR 0004). These numbers are printed in the job log, but nobody reads job logs month after month.
A change in the source shows up as a change in these counts, and it should be visible without opening a log.

Loading January to March 2025 already showed such a change: rule W003 (zero base fare) matched 0.0062% of rows
in February and 0.31% in March, about 50 times more. Nothing failed, because W003 is a warning, so the change
was only visible by reading the numbers side by side.

The project targets companies that use Databricks, and the tables already live in Unity Catalog.

## Decision

- **Databricks AI/BI dashboard**, on the serverless SQL warehouse of the workspace. It reads the Unity Catalog
  tables directly; nothing is copied out of Databricks.
- **The first page is about data quality**, not business metrics: rows checked and quarantined per month, the
  match rate of every rule per month, and quarantined rows per reject rule and company.
- **A spike** is a rule whose match rate is at least twice the previous month's rate while matching at least
  100 rows. The row minimum keeps small numbers (1 row becoming 3) from being reported as spikes.
- **The queries are SQL files in the repository** (`dashboards/queries`). Tests check that they only read
  tables the pipeline writes and that the company mapping matches gold (ADR 0008).
- **The dashboard is code.** It is built in the Databricks editor, exported to the repository with
  `databricks bundle generate dashboard`, and deployed with the Asset Bundle like the job (ADR 0009).
- **Six months of data**, January to June 2025, are loaded for the dashboard. January 2025 is the first month
  with the CBD congestion fee, so the six months share one schema.

## Consequences

- A change in the source (a new error pattern, a rule that suddenly matches far more rows) is visible as a
  spike on one page.
- The dashboard only reads small tables (rule counts) and the quarantine table (hundreds to a few thousand rows
  per month), so it is cheap to refresh on the Free Edition quota.
- Table names in the queries are fixed to `workspace.mobility`. Another catalog or schema means editing the
  queries; this is acceptable while there is one environment.
- Business pages (trips per company, busy zones) can be added later on the gold tables without changing this
  page.

## Alternatives considered

- **Power BI.** Common in companies, but needs Power BI Desktop and a separate connection, and `.pbix` files
  cannot be reviewed in Git. Rejected.
- **Streamlit.** Flexible, but needs its own hosting and overlaps with the API planned for Sprint 6. Rejected.
- **Alerts on the rule counts only.** Useful later, but an alert without a history to look at does not explain
  what changed. The dashboard comes first.
