USE DATABASE HVFHS_DB;
USE WAREHOUSE HVFHS_WH;

-- Check 1: no trips with dropoff before/equal to pickup should exist in GOLD
SELECT COUNT(*) AS bad_time_order
FROM GOLD.FACT_TRIPS
WHERE dropoff_datetime <= pickup_datetime;

-- Check 2: no unmapped providers
SELECT COUNT(*) AS unmapped_providers
FROM GOLD.FACT_TRIPS
WHERE provider_name IS NULL;

-- Check 3: zone join integrity — flag if match rate drops (e.g. new zone IDs)
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN pickup_borough IS NULL THEN 1 ELSE 0 END) AS unmatched_pickup_zone,
    SUM(CASE WHEN dropoff_borough IS NULL THEN 1 ELSE 0 END) AS unmatched_dropoff_zone
FROM GOLD.FACT_TRIPS;

-- Check 4: row count sanity vs raw (flags upstream ingestion issues)
SELECT
    (SELECT COUNT(*) FROM RAW.TRIPS) AS raw_rows,
    (SELECT COUNT(*) FROM GOLD.FACT_TRIPS) AS gold_rows,
    (SELECT COUNT(*) FROM RAW.TRIPS) - (SELECT COUNT(*) FROM GOLD.FACT_TRIPS) AS rows_filtered;
