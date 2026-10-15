# 0014. Star schema in gold, exported for Power BI

- Status: Accepted, validated on Databricks for January to June 2025
- Date: 2026-10-15
- Decider: Nafiul Hadi Saputra

## Context

The gold tables of ADR 0008 answer three fixed questions. A BI tool needs something different: one model that
answers questions nobody has asked yet, sliced by date, hour, zone and company, with the same totals whichever way
it is sliced. Power BI works best with a star schema: narrow fact tables with numbers and keys, surrounded by small
dimension tables with descriptions.

Three facts about the source shape the design:

- The TLC data has **no trip ID**, and fully identical rows exist. A key cannot be taken from the source.
- Pipelines are rerun (ADR 0003). A key that is a running number would change on every rerun and break every
  saved relationship and report.
- One month of trips is about 20 million rows. Power BI Desktop on a laptop can import one month at trip grain,
  but not six.

## Decision

### Tables and grain

The grain is what one row means. Every table states it, and the checks below prove it.

| Table | Type | Grain | Key |
|---|---|---|---|
| `fct_trips` | Fact | One trip that passed every reject rule | `trip_key` |
| `fct_trips_hourly` | Aggregate fact | Pickup date x pickup hour x pickup zone x company | the four keys together |
| `dim_date` | Dimension | One calendar day, 2024-01-01 to 2027-12-31 | `date_key` = yyyymmdd |
| `dim_time` | Dimension | One hour of the day (24 rows) | `time_key` = hour |
| `dim_zone` | Dimension | One TLC taxi zone | `zone_key` = TLC `LocationID` |
| `dim_company` | Dimension | One HVFHS licence code | `company_key` = hash of the code |

`fct_trips` has two keys to `dim_zone`: `pickup_zone_key` and `dropoff_zone_key`. The same dimension plays two
roles (a **role-playing dimension**), so there is one zone table to maintain instead of two copies.

### Keys are computed from values, never numbered

- `date_key`, `time_key` and `zone_key` are the natural values themselves (20250115, 18, 132).
- `company_key` is `xxhash64` of the licence code.
- `trip_key` is `xxhash64` of all 25 source columns plus a sequence number inside a group of identical rows.
  Identical trips get different keys, and a rerun gives the same keys.

The same input always gives the same keys, so a rerun never breaks a relationship.

### What enters the facts

- `fct_trips` is built from one silver month, so **quarantined rows never reach the facts** (ADR 0004).
- `fct_trips_hourly` is built from `fct_trips`, not from silver, so its totals always reconcile with the trip facts.
- `fct_trips_hourly` stores **sums and counts only**, never averages. An average is computed from totals in the
  report (ADR 0008). Average wait time is `wait_time_seconds_sum / wait_time_trips`, where `wait_time_trips` counts
  only the trips with a valid wait time.

### Definitions

- **`total_fare`** is what the passenger paid, with the same definition as gold `passenger_paid_total`: base fare,
  tolls, Black Car Fund, sales tax, congestion surcharge, airport fee, CBD congestion fee and tips. The CBD fee is
  null before 2025 (ADR 0006) and adds nothing for those months.
- **`wait_time_seconds`** is pickup time minus request time. It is null when the request time is missing or the
  pickup is before the request (warning rule W001). A negative wait is a recording error, and counting it as zero
  would pull the average down.
- **Peak hours** are 07:00-09:59 and 16:00-19:59 (`dim_time.is_peak_hour`). The flag describes the hour only;
  combine it with `dim_date.is_weekend` for weekday rush hours.
- **Airports** are the zones whose `service_zone` is `Airports` or `EWR`: JFK (132), LaGuardia (138) and Newark (1).
- **Companies** use the names of ADR 0008. Unknown licence codes are kept as `Other (<code>)`.
- Date and hour come from the **pickup time, New York local time** (ADR 0006).

### Write strategies

| Table | Strategy | Why |
|---|---|---|
| `fct_trips`, `fct_trips_hourly` | Replace one month (ADR 0003) | Same as every other table |
| `dim_date`, `dim_time`, `dim_zone` | Replace the whole table | Small, fully rebuilt from complete inputs |
| `dim_company` | `MERGE` on the licence code | A month only shows the codes it contains; replacing would drop codes seen in other months |

The dimensions are written before the facts. A month outside the calendar stops the run before anything is written,
instead of producing facts without a date.

### Automatic checks

After every build the `star` step checks the tables as written. The CLI exits with 1 and the Databricks task fails
when any check fails:

1. Silver trips of the month = rows in `fct_trips`.
2. Unique `trip_key` values = rows in `fct_trips` (the grain holds).
3. Trips in `fct_trips_hourly` = rows in `fct_trips` (the aggregate reconciles).
4. Zero fact keys without a dimension row, for each of the five keys (no orphan keys).

### Power BI export

Power BI reads Parquet files from disk. The `powerbi-export` command (locally) and the `export_powerbi` job (on
Databricks) write one file per table:

- every dimension, with `dim_date` limited to the whole years that have data, so Power BI accepts it as a date table;
- `fct_trips_hourly` for every month that is built;
- `fct_trips` for one month only, without `trip_key`, which the dashboard does not use and which would be the
  most expensive column for Power BI to store.

## Consequences

Measured on Databricks Free Edition (serverless) for January to June 2025:

| Month | `fct_trips` rows | All 8 checks |
|---|---|---|
| 2025-01 | 20,402,922 | passed |
| 2025-02 | 19,338,701 | passed |
| 2025-03 | 20,536,446 | passed |
| 2025-04 | 19,753,594 | passed |
| 2025-05 | 21,090,811 | passed |
| 2025-06 | 19,867,489 | passed |
| **Total** | **120,989,963** | |

- Trips without a valid wait time: 199,760 in January (0.98%) and 231,327 in June (1.16%).
- `dim_zone` has 265 rows, `dim_company` 4, `dim_time` 24, `dim_date` 1,461.
- January 2025 gives 20,402,922 trips in `fct_trips` both locally in Docker and on Databricks, with all checks
  passed in both places.
- The Power BI export from Databricks (trip grain for June, hourly grain for six months) is 444.6 MB: 353.4 MB for
  `fct_trips` (19,867,489 rows) and 91.2 MB for `fct_trips_hourly` (2,139,836 rows). The dimensions are under
  0.1 MB together. The hourly fact has about 9 times fewer rows than one month of trips while covering six months,
  so most dashboard pages read it.
- The calendar ends on 2027-12-31. Loading 2028 data needs `CALENDAR_END` extended first; the run says so.
- `MERGE` on `dim_company` means the table keeps every code ever seen, even after a month is reprocessed.

## Alternatives considered

- **Running-number surrogate keys** (`row_number()` or an identity column). The textbook choice, but the numbers
  change when a month is rebuilt, and every fact of every other month would need rewriting. Rejected.
- **No trip key; let the trip grain be implicit.** Simpler, but duplicates could not be told apart from double
  loads, and the uniqueness check would be impossible. Rejected.
- **Separate pickup and dropoff zone tables.** What some tools need, but Power BI handles a role-playing dimension
  with one active and one inactive relationship. One table keeps both roles consistent. Rejected.
- **Storing averages in the hourly fact.** An average of averages weighs a quiet hour like a busy one. Rejected.
- **Power BI reading Databricks directly (DirectQuery).** The production choice for a company, but it needs a
  running SQL warehouse and access tokens on every viewer's machine. Files keep the dashboard reproducible from
  the repository. Can be added later without changing the model.
- **Exporting every month at trip grain.** About 120 million rows for six months: too large for Power BI Desktop
  on a laptop, and not needed, because the hourly fact answers the same questions. Rejected.
