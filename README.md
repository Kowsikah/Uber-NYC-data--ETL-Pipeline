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
- **Every transformation was validated against real data** — see the
  "Validated results" section below. Logic was proven first on the
  January 2021 file (11.9M rows, 100% zone-join match rate, <0.001%
  reject rate on the quality filter) before being run across the full year.

## Validated results (full year 2021)

Run against the complete 12-month load (`RAW.TRIPS`, ~174.6M raw rows):

| Check | Result |
|---|---|
| Total rows in `GOLD.FACT_TRIPS` | **174,588,481** |
| Bad time order (`dropoff <= pickup`) | **0** |
| Unmapped providers (`provider_name IS NULL`) | **0** |
| Unmatched pickup zones | **0** |
| Unmatched dropoff zones | **0** |
| `SP_QUALITY_CHECK()` task status | **Passed** (no bad rows found post-rebuild) |

Zone lookup load: 265/265 rows loaded from `taxi_zone_lookup.csv`, 0 errors.

Provider mapping (`GOLD.DIM_PROVIDERS`):

| hvfhs_license_num | provider_name |
|---|---|
| HV0003 | Uber |
| HV0005 | Lyft |
| HV0004 | Via |

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
setup_raw_action.sql   -- DB, warehouse, schemas, internal stages, RAW tables
staging_schema.sql     -- typed/cleaned STAGING views
intermediate_gold.sql  -- zone-enriched view + GOLD star schema
quality-check.sql      -- standalone ad hoc quality queries
sample_analysis.sql    -- business-question queries (also used as Power BI source queries)
orchestration.sql      -- stored procedures + Task graph (monthly schedule)
README.md
```

## Setup

1. **Snowflake**: sign up at snowflake.com (no card needed), note your account URL.
2. Run `setup_raw_action.sql` in a Snowflake worksheet to create the
   database, warehouse, schemas, internal stages, and raw tables.
3. From SnowSQL CLI, load each month's files:
   ```bash
   snowsql -a <account> -u <user>
   PUT file://./data/fhvhv_tripdata_2021-01.parquet @RAW.HVFHS_STAGE;
   PUT file://./data/taxi_zone_lookup.csv @RAW.ZONES_STAGE;
   ```
   Then run the `COPY INTO` statements at the bottom of `setup_raw_action.sql`.
4. Run `staging_schema.sql`, then `intermediate_gold.sql`.
5. Run `quality-check.sql` — all checks should return 0 bad rows.
6. Run `sample_analysis.sql` for the business-question queries — these
   double as your Power BI dataset queries.

## Orchestration (Snowflake Tasks)

```sql
-- Run once to create the stored procedures and Task graph:
orchestration.sql
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
`sample_analysis.sql`.

**Dashboard covers (full year 2021, 174,588,481 trips loaded):**
- Executive KPIs: total trips, total revenue, total driver pay, avg fare
- Provider market share (Uber ~70.8%, Lyft ~28.6%, Via ~0.6%) and driver pay by provider
- Monthly trend: trips, revenue, and avg fare by `_source_month`
- Pickup × dropoff borough cross-tab (cross-borough travel patterns)
- Top pickup/dropoff zones by trip count and revenue
- Airport vs non-airport trip split (~6.4% airport trips)

## Scale story

- With multiple months loaded, run
  `SELECT SYSTEM$CLUSTERING_INFORMATION('GOLD.FACT_TRIPS')` before/after the
  `CLUSTER BY (pickup_datetime)` step and record the query-time difference.
- Trade-off worth calling out explicitly: internal stage + Snowflake SQL
  instead of Databricks/ADLS, and Snowflake Tasks instead of Airflow — both
  chosen for zero infra cost and no cross-platform auth to manage, while
  still demonstrating the same medallion architecture and DAG-based
  orchestration pattern used in production, at a genuine scale of nearly
  175M rows.
