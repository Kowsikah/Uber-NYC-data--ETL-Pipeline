CREATE DATABASE IF NOT EXISTS HVFHS_DB;
CREATE WAREHOUSE IF NOT EXISTS HVFHS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE DATABASE HVFHS_DB;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS GOLD;

USE WAREHOUSE HVFHS_WH;

-- Internal stage: no cloud storage account needed. Files are PUT
-- directly from your local machine straight into Snowflake.
CREATE OR REPLACE STAGE RAW.HVFHS_STAGE
    FILE_FORMAT = (TYPE = PARQUET);

CREATE OR REPLACE STAGE RAW.ZONES_STAGE
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Raw trips table — one row per trip, schema matches the TLC data dictionary.
CREATE OR REPLACE TABLE RAW.TRIPS (
    hvfhs_license_num        STRING,
    dispatching_base_num     STRING,
    originating_base_num     STRING,
    request_datetime         TIMESTAMP_NTZ,
    on_scene_datetime        TIMESTAMP_NTZ,
    pickup_datetime          TIMESTAMP_NTZ,
    dropoff_datetime         TIMESTAMP_NTZ,
    PULocationID              INTEGER,
    DOLocationID              INTEGER,
    trip_miles                FLOAT,
    trip_time                 INTEGER,
    base_passenger_fare       FLOAT,
    tolls                     FLOAT,
    bcf                       FLOAT,
    sales_tax                 FLOAT,
    congestion_surcharge      FLOAT,
    airport_fee                FLOAT,
    tips                       FLOAT,
    driver_pay                 FLOAT,
    shared_request_flag        STRING,
    shared_match_flag          STRING,
    access_a_ride_flag         STRING,
    wav_request_flag            STRING,
    wav_match_flag               STRING,
    _source_month               STRING,
    _loaded_at                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.ZONE_LOOKUP (
    LocationID    INTEGER,
    Borough       STRING,
    Zone          STRING,
    service_zone  STRING
);

SELECT _source_month, COUNT(*) AS trips
FROM RAW.TRIPS
GROUP BY 1
ORDER BY 1;