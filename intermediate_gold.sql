-- ============================================================
-- 03_intermediate_and_gold.sql
-- Zone-enriched intermediate view + star-schema gold marts.
-- Validated locally: 0 unmatched pickup/dropoff zones on the
-- real January 2021 file (11,908,415 rows, 100% join match).
-- ============================================================

USE DATABASE HVFHS_DB;
USE WAREHOUSE HVFHS_WH;

CREATE OR REPLACE VIEW INTERMEDIATE.INT_TRIPS_ZONED AS
SELECT
    t.*,
    pu.borough AS pickup_borough,
    pu.zone    AS pickup_zone,
    dz.borough AS dropoff_borough,
    dz.zone    AS dropoff_zone,
    DATEDIFF('second', t.pickup_datetime, t.dropoff_datetime) / 60.0 AS trip_duration_min
FROM STAGING.STG_TRIPS t
LEFT JOIN STAGING.STG_ZONES pu ON t.pu_location_id = pu.location_id
LEFT JOIN STAGING.STG_ZONES dz ON t.do_location_id = dz.location_id;

-- Fact table
CREATE OR REPLACE TABLE GOLD.FACT_TRIPS AS
SELECT * FROM INTERMEDIATE.INT_TRIPS_ZONED;

-- Dimension: providers
CREATE OR REPLACE TABLE GOLD.DIM_PROVIDERS AS
SELECT DISTINCT
    hvfhs_license_num,
    provider_name
FROM STAGING.STG_TRIPS;

-- Dimension: zones
CREATE OR REPLACE TABLE GOLD.DIM_ZONES AS
SELECT * FROM STAGING.STG_ZONES;

-- Dimension: date (generated, covers 2019-2025 for the full HVFHS history)
CREATE OR REPLACE TABLE GOLD.DIM_DATE AS
SELECT
    d::DATE AS date_key,
    YEAR(d) AS year,
    MONTH(d) AS month,
    DAY(d) AS day,
    DAYOFWEEK(d) AS day_of_week,
    DAYNAME(d) AS day_name,
    CASE WHEN DAYOFWEEK(d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    SELECT DATEADD('day', SEQ4(), '2019-01-01') AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 2600))
);

-- Cluster the fact table on pickup date for query performance at scale.
-- Run this once you have multiple months loaded, then compare
-- SYSTEM$CLUSTERING_INFORMATION before/after for your scale writeup.
ALTER TABLE GOLD.FACT_TRIPS CLUSTER BY (pickup_datetime);
