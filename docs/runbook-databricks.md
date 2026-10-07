# Runbook: running the pipeline on Databricks

How to run the monthly pipeline on Databricks Free Edition ([ADR 0009](adr/0009-running-on-databricks.md)).
Commands are for Windows Command Prompt and use the Databricks CLI profile `mobility`.

## One-time setup

1. Check the catalog name of your workspace (Free Edition normally has `workspace`):

   ```
   databricks catalogs list --profile mobility
   ```

   If it is not `workspace`, pass `--var catalog=<name>` to every `bundle` command below.

2. Create the schema and the volume for raw files:

   ```
   databricks schemas create mobility workspace --profile mobility
   databricks volumes create workspace mobility raw MANAGED --profile mobility
   ```

3. Create the folders inside the volume. `databricks fs cp` does not create missing folders in a volume and
   fails with `no such directory`:

   ```
   databricks fs mkdir dbfs:/Volumes/workspace/mobility/raw/fhvhv --profile mobility
   databricks fs mkdir dbfs:/Volumes/workspace/mobility/raw/reference --profile mobility
   ```

## Every month

1. Download the month and the zone lookup locally:

   ```
   uv run mobility-lakehouse download --month 2025-01
   ```

2. Upload both to the volume. Databricks Free Edition restricts outbound internet access, so the job cannot
   download from the TLC itself:

   ```
   databricks fs cp data\raw\fhvhv\fhvhv_tripdata_2025-01.parquet dbfs:/Volumes/workspace/mobility/raw/fhvhv/fhvhv_tripdata_2025-01.parquet --overwrite --profile mobility
   databricks fs cp data\raw\reference\taxi_zone_lookup.csv dbfs:/Volumes/workspace/mobility/raw/reference/taxi_zone_lookup.csv --overwrite --profile mobility
   ```

3. Deploy the bundle (builds the wheel) and run the job for that month:

   ```
   databricks bundle validate --profile mobility
   databricks bundle deploy --profile mobility
   databricks bundle run monthly_pipeline --profile mobility --params month=2025-01
   ```

   The job runs three tasks in order: `bronze`, `silver`, `gold`. Rerunning a month replaces that month only.

4. Check the result in the SQL editor:

   ```sql
   SELECT company, trips, driver_pay_per_mile, driver_pay_per_minute
   FROM workspace.mobility.gold_fhvhv_monthly_driver_economics
   WHERE data_month = DATE'2025-01-01';
   ```

   The trip counts must match the local run of the same month. To check every layer at once:

   ```sql
   SELECT 'bronze' AS layer, COUNT(*) AS row_count FROM workspace.mobility.bronze_fhvhv_trips
   UNION ALL SELECT 'silver', COUNT(*) FROM workspace.mobility.silver_fhvhv_trips
   UNION ALL SELECT 'quarantine', COUNT(*) FROM workspace.mobility.quarantine_fhvhv_trips
   UNION ALL SELECT 'rule_counts', COUNT(*) FROM workspace.mobility.quality_fhvhv_rule_counts
   UNION ALL SELECT 'gold_daily', COUNT(*) FROM workspace.mobility.gold_fhvhv_daily_company_trips;
   ```

## Tables created

| Layer | Table |
|---|---|
| Bronze | `workspace.mobility.bronze_fhvhv_trips` |
| Silver | `workspace.mobility.silver_fhvhv_trips` |
| Quarantine | `workspace.mobility.quarantine_fhvhv_trips` |
| Rule counts | `workspace.mobility.quality_fhvhv_rule_counts` |
| Gold | `workspace.mobility.gold_fhvhv_daily_company_trips`, `..._hourly_pickup_zones`, `..._monthly_driver_economics` |

## When something goes wrong

| Symptom | Likely cause | What to do |
|---|---|---|
| `fs cp` fails with `no such directory` | The folder does not exist in the volume | Run the `fs mkdir` commands of the one-time setup |
| `bronze` fails with a path not found error | The raw file is not in the volume | Repeat step 2 for that month |
| `silver` fails with "no bronze rows" | `bronze` did not run for that month | Rerun the whole job for the month |
| The job runs old code after a change | Serverless reused a cached environment | Deploy again; `dynamic_version` gives each deploy a new wheel version |
| Compute is unavailable for the rest of the day | The Free Edition daily quota was exceeded | Wait until the next day; run one month at a time |
| `bundle deploy` fails on `uv build` | uv is not on the PATH of this terminal | Open a new Command Prompt and check `uv --version` |
