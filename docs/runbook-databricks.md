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

4. Create the volume for the API snapshot ([ADR 0011](adr/0011-api-over-a-parquet-snapshot.md)). Spark creates
   the folders inside it:

   ```
   databricks volumes create workspace mobility serving MANAGED --profile mobility
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

   The job runs four tasks in order: `bronze`, `silver`, `gold`, `snapshot`. Rerunning a month replaces that
   month only.

   Several months, one after the other (Command Prompt; the job allows one run at a time):

   ```
   for %m in (2025-02 2025-03 2025-04) do databricks bundle run monthly_pipeline --profile mobility --params month=%m
   ```

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

## API snapshot

The `snapshot` task writes the month as Parquet to `/Volumes/workspace/mobility/serving/snapshot`. For months
whose gold tables already exist, run only the snapshot job:

```
for %m in (2025-01 2025-02 2025-03 2025-04 2025-05 2025-06) do databricks bundle run export_snapshot --profile mobility --params month=%m
```

Download the whole snapshot for the API. Delete the local copy first: a rewritten month has new file names,
and old files left next to them would be read twice.

```
rmdir /s /q data\snapshot
databricks fs cp -r dbfs:/Volumes/workspace/mobility/serving/snapshot data\snapshot --profile mobility
docker compose up --build api
```

The API is on http://localhost:8000 and its documentation on http://localhost:8000/docs.

## Data quality dashboard

The dashboard (ADR 0010) is deployed with the bundle, next to the job:

```
databricks bundle deploy --profile mobility
```

It appears under **Dashboards** with a `[dev <user>]` prefix while the `dev` target is used. To change it, edit
the deployed dashboard in the Databricks editor, then export the change back into the repository and commit it:

```
databricks bundle generate dashboard --resource quality_dashboard --force --profile mobility
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
| `bundle validate` cannot find the warehouse | The SQL warehouse has another name | Pass `--var warehouse_id=<id>`, or change the lookup in `databricks.yml` |
| `snapshot` fails with a volume not found error | The `serving` volume does not exist | Run step 4 of the one-time setup |
| The API does not start: `no Parquet files in ...` | The snapshot was not downloaded, or only partly | Download the snapshot again (see API snapshot) |
| `bundle deploy` fails on `uv build` | uv is not on the PATH of this terminal | Open a new Command Prompt and check `uv --version` |
