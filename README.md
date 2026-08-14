# Uber/HVFHS NYC Trip Data — ETL Pipeline

An end-to-end data engineering pipeline for NYC's High Volume For-Hire Vehicle
(HVFHS) trip data — covering Uber, Lyft, and Via — built on **Snowflake**,
orchestrated natively with **Snowflake Tasks**, and visualized in **Power BI**.

No external cloud infrastructure account required beyond a free Snowflake
trial (no credit card, $400 credit / 30 days). Everything else runs locally.

## Architecture

```
Local parquet files (TLC monthly HVFHS trip data)
      │  PUT
      ▼
Snowflake internal stage
      │  COPY INTO
      ▼
RAW.TRIPS, RAW.ZONE_LOOKUP
      │  SQL views
      ▼
STAGING (typed, cleaned, business-friendly columns)
      │
      ▼
INTERMEDIATE (zone-enriched: pickup/dropoff borough + zone)
      │
      ▼
GOLD (star schema: FACT_TRIPS, DIM_ZONES, DIM_PROVIDERS, DIM_DATE)
      │
      ├──► Snowflake Tasks (native orchestration + quality gate, monthly)
      └──► Power BI (native Snowflake connector)
```

## Why this design

- **No Databricks, no ADLS, no external stage credentials.** The SQL
  transformation logic (staging → intermediate → gold, quality gates,
  clustering) is the same pattern used in production Snowflake-native
  pipelines, without the extra infra to provision or pay for.
- **Orchestration runs natively inside Snowflake** via a Task graph
  (`GOLD.TASK_REBUILD_GOLD` → `GOLD.TASK_QUALITY_CHECK`), not an external
  scheduler. Tasks call stored procedures (`SP_REBUILD_GOLD`,
  `SP_QUALITY_CHECK`) and their run history is queryable through
  `INFORMATION_SCHEMA.TASK_HISTORY()` — the same observability an Airflow
  Graph view would give, with one less moving part to operate.
  - **Scope note:** Tasks execute SQL/stored procedures inside Snowflake.
    They can't reach a local filesystem to run `PUT`, so loading new monthly
    files into the stage stays a manual/external step no matter what
    orchestrates the transform layer.
- **Every transformation was validated against real data before being
  written as Snowflake SQL** — see `validation/validation_report.md`. The
  January 2021 file (11.9M rows) was run through the same logic locally
  first, confirming a 100% zone-join match rate and a sub-0.001% reject rate
  on the quality filter before ever touching the warehouse.

## Data quality notes

- `pickup_borough` / `dropoff_borough` / `pickup_zone` / `dropoff_zone` use
  `COALESCE(..., 'Unknown')` in `INT_TRIPS_ZONED` to guarantee no NULLs reach
  the gold layer.
- Separately, TLC's own zone lookup table includes real placeholder zones —
  `LocationID` 264 (`"Unknown"`) and 265 (`"N/A"`) — for trips that couldn't
  be pinned to a specific NYC zone. These are legitimate values, not join
  failures, and represent well under 0.01% of trips across the full year.
  Dashboards filter them out of ranked zone/borough visuals but keep them in
  overall trip-count totals for accuracy.

## Repo structure

```
sql/
  01_create_stage_and_raw.sql     -- DB, warehouse, schemas, internal stages, RAW tables
  02_staging.sql                  -- typed/cleaned STAGING views
  03_intermediate_and_gold.sql    -- zone-enriched view + GOLD star schema
  04_quality_checks.sql           -- standalone ad hoc quality queries
  05_sample_analysis.sql          -- business-question queries (also used as Power BI source queries)
  07_snowflake_tasks_orchestration.sql  -- stored procedures + Task graph (monthly schedule)
validation/
  validation_report.md            -- local validation results against real Jan 2021 data
README.md
```

## Setup

1. **Snowflake**: sign up at snowflake.com (no card needed), note your account URL.
2. Run `sql/01_create_stage_and_raw.sql` in a Snowflake worksheet to create the
   database, warehouse, schemas, internal stages, and raw tables.
3. From SnowSQL CLI, load each month's files:
   ```bash
   snowsql -a <account> -u <user>
   PUT file://./data/fhvhv_tripdata_2021-01.parquet @RAW.HVFHS_STAGE;
   PUT file://./data/taxi_zone_lookup.csv @RAW.ZONES_STAGE;
   ```
   Then run the `COPY INTO` statements at the bottom of `01_create_stage_and_raw.sql`.
4. Run `sql/02_staging.sql`, then `sql/03_intermediate_and_gold.sql`.
5. Run `sql/04_quality_checks.sql` — all checks should return 0 bad rows.
6. Run `sql/05_sample_analysis.sql` for the business-question queries — these
   double as your Power BI dataset queries.

## Orchestration (Snowflake Tasks)

```sql
-- Run once to create the stored procedures and Task graph:
sql/07_snowflake_tasks_orchestration.sql
```

This creates:
- `GOLD.SP_REBUILD_GOLD()` — rebuilds `FACT_TRIPS`, `DIM_PROVIDERS`,
  `DIM_ZONES` from staging/intermediate, and re-clusters the fact table.
- `GOLD.SP_QUALITY_CHECK()` — raises an exception (visible as a FAILED task
  run) if any bad rows are found post-rebuild.
- `GOLD.TASK_REBUILD_GOLD` (scheduled 1st of each month, 6am UTC) →
  `GOLD.TASK_QUALITY_CHECK` (runs `AFTER` the rebuild task).

Tasks are created suspended. To activate the graph, resume child-to-parent:
```sql
ALTER TASK GOLD.TASK_QUALITY_CHECK RESUME;
ALTER TASK GOLD.TASK_REBUILD_GOLD RESUME;
```

To trigger a rebuild manually (don't wait for the monthly schedule during dev):
```sql
EXECUTE TASK GOLD.TASK_REBUILD_GOLD;
```

Monitor runs the same way you'd read an Airflow Graph view:
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 50
)) ORDER BY SCHEDULED_TIME DESC;
```

## Power BI

Get Data → Snowflake → account URL + warehouse `HVFHS_WH` → select
`GOLD.FACT_TRIPS`, `GOLD.DIM_ZONES`, `GOLD.DIM_PROVIDERS`, `GOLD.DIM_DATE` →
build relationships in Model view → dashboard the queries in
`sql/05_sample_analysis.sql`.

**Dashboard covers (full year 2021, ~174.6M trips loaded):**
- Executive KPIs: total trips, total revenue, total driver pay, avg fare
- Provider market share (Uber ~71%, Lyft ~28%, Via <1%) and driver pay by provider
- Monthly trend: trips, revenue, and avg fare by `_source_month`
- Pickup × dropoff borough cross-tab (cross-borough travel patterns)
- Top pickup/dropoff zones by trip count and revenue
- Airport vs non-airport trip split

## Scale story

- With multiple months loaded, run
  `SELECT SYSTEM$CLUSTERING_INFORMATION('GOLD.FACT_TRIPS')` before/after the
  `CLUSTER BY (pickup_datetime)` step and record the query-time difference.
- Trade-off worth calling out explicitly: internal stage + Snowflake SQL
  instead of Databricks/ADLS, and Snowflake Tasks instead of Airflow — both
  chosen for zero infra cost and no cross-platform auth to manage, while
  still demonstrating the same medallion architecture and DAG-based
  orchestration pattern used in production.
