-- ============================================================
-- 07_snowflake_tasks_orchestration.sql
-- Replaces Airflow entirely. Orchestration for the transform +
-- quality-check portion of the pipeline runs natively inside
-- Snowflake as a Task graph — no external scheduler needed.
--
-- Honest scope note: Tasks execute SQL/stored procedures inside
-- Snowflake. They cannot reach your local D:\parquet_files to run
-- PUT — that step stays external no matter what orchestrates the
-- rest (see the note at the bottom of this file).
-- ============================================================

USE DATABASE HVFHS_DB;
USE WAREHOUSE HVFHS_WH;

-- ------------------------------------------------------------
-- Stored procedure: rebuild the gold layer
-- ------------------------------------------------------------
CREATE OR REPLACE PROCEDURE GOLD.SP_REBUILD_GOLD()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    CREATE OR REPLACE TABLE GOLD.FACT_TRIPS AS
    SELECT * FROM INTERMEDIATE.INT_TRIPS_ZONED;

    CREATE OR REPLACE TABLE GOLD.DIM_PROVIDERS AS
    SELECT DISTINCT hvfhs_license_num, provider_name FROM STAGING.STG_TRIPS;

    CREATE OR REPLACE TABLE GOLD.DIM_ZONES AS
    SELECT * FROM STAGING.STG_ZONES;

    ALTER TABLE GOLD.FACT_TRIPS CLUSTER BY (pickup_datetime);

    RETURN 'Gold layer rebuilt successfully';
END;
$$;

-- ------------------------------------------------------------
-- Stored procedure: quality gate. Raises an exception (not just
-- a warning) on failure, so the Task shows FAILED in TASK_HISTORY
-- and can trigger an error notification (see optional block below).
-- ------------------------------------------------------------
CREATE OR REPLACE PROCEDURE GOLD.SP_QUALITY_CHECK()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    bad_count INTEGER;
    quality_failure EXCEPTION (-20001, 'HVFHS data quality check failed');
BEGIN
    SELECT COUNT(*) INTO :bad_count
    FROM GOLD.FACT_TRIPS
    WHERE dropoff_datetime <= pickup_datetime
       OR provider_name IS NULL
       OR pickup_borough IS NULL
       OR dropoff_borough IS NULL;

    IF (bad_count > 0) THEN
        RAISE quality_failure;
    END IF;

    RETURN 'All quality checks passed, 0 bad rows out of ' ||
           (SELECT COUNT(*) FROM GOLD.FACT_TRIPS)::STRING;
END;
$$;

-- ------------------------------------------------------------
-- Task graph: root task (scheduled) -> quality check (AFTER root)
-- ------------------------------------------------------------
CREATE OR REPLACE TASK GOLD.TASK_REBUILD_GOLD
    WAREHOUSE = HVFHS_WH
    SCHEDULE = 'USING CRON 0 6 1 * * UTC'   -- 1st of each month, 6am UTC
    COMMENT = 'Rebuilds GOLD layer from STAGING/INTERMEDIATE views'
AS
    CALL GOLD.SP_REBUILD_GOLD();

CREATE OR REPLACE TASK GOLD.TASK_QUALITY_CHECK
    WAREHOUSE = HVFHS_WH
    AFTER GOLD.TASK_REBUILD_GOLD
  
AS
    CALL GOLD.SP_QUALITY_CHECK();

-- IMPORTANT: Snowflake requires every task in a DAG to be resumed
-- before the root task — resume child-to-parent order, root last.
ALTER TASK GOLD.TASK_QUALITY_CHECK suspend;
ALTER TASK GOLD.TASK_REBUILD_GOLD suspend;

-- ------------------------------------------------------------
-- Manual trigger (don't wait for the monthly schedule during dev/demo)
-- ------------------------------------------------------------
-- EXECUTE TASK GOLD.TASK_REBUILD_GOLD;

-- ------------------------------------------------------------
-- Monitoring — this is your equivalent of the Airflow Graph view
-- ------------------------------------------------------------
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 50
))
ORDER BY SCHEDULED_TIME DESC;

SHOW TASKS IN SCHEMA GOLD;


 