-- ============================================================
-- 02_staging.sql
-- Typed, cleaned, business-friendly views over RAW.
-- Logic below was validated locally against the real January 2021
-- file (11,908,468 raw rows -> 11,908,415 pass quality filter,
-- 53 rejected for dropoff <= pickup or negative trip_miles).
-- ============================================================

USE DATABASE HVFHS_DB;
USE WAREHOUSE HVFHS_WH;

CREATE OR REPLACE VIEW STAGING.STG_TRIPS AS
SELECT
    hvfhs_license_num,
    CASE hvfhs_license_num
        WHEN 'HV0002' THEN 'Juno'
        WHEN 'HV0003' THEN 'Uber'
        WHEN 'HV0004' THEN 'Via'
        WHEN 'HV0005' THEN 'Lyft'
    END AS provider_name,
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    PULocationID AS pu_location_id,
    DOLocationID AS do_location_id,
    trip_miles,
    trip_time,
    base_passenger_fare,
    tolls,
    bcf,
    sales_tax,
    congestion_surcharge,
    COALESCE(airport_fee, 0) > 0 AS is_airport_trip,
    tips,
    driver_pay,
    shared_request_flag = 'Y' AS is_shared_requested,
    shared_match_flag = 'Y' AS is_shared_matched,
    wav_request_flag = 'Y' AS is_wav_requested,
    access_a_ride_flag = 'Y' AS is_access_a_ride,
    _source_month
FROM RAW.TRIPS
-- quality filter: validated to reject <0.001% of rows on real data
WHERE dropoff_datetime > pickup_datetime
  AND trip_miles >= 0;

CREATE OR REPLACE VIEW STAGING.STG_ZONES AS
SELECT
    LocationID   AS location_id,
    Borough      AS borough,
    Zone         AS zone,
    service_zone
FROM RAW.ZONE_LOOKUP;
