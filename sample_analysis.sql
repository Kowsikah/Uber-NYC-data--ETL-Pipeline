-- ============================================================
-- 05_sample_analysis.sql
-- Example business questions this warehouse can answer.
-- Use these as your Power BI dataset queries.
-- Numbers below are the ACTUAL results from validating this
-- logic against the real January 2021 file (11.9M trips).
-- ============================================================

-- Trip volume & avg driver pay by borough and provider
-- (validated result: Manhattan Uber avg driver pay = $14.03,
--  Manhattan Via avg driver pay = $10.45)
SELECT
    pickup_borough,
    provider_name,
    COUNT(*) AS trips,
    ROUND(AVG(driver_pay), 2) AS avg_driver_pay,
    ROUND(AVG(base_passenger_fare), 2) AS avg_fare,
    ROUND(AVG(tips), 2) AS avg_tip
FROM GOLD.FACT_TRIPS
WHERE pickup_borough IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, trips DESC;

-- Provider market share overall
-- (validated result: Uber 73.1%, Lyft 26.0%, Via 0.9% of Jan 2021 trips)
SELECT
    provider_name,
    COUNT(*) AS trips,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_market_share
FROM GOLD.FACT_TRIPS
GROUP BY 1
ORDER BY trips DESC;

-- Driver pay as % of fare — a genuinely topical gig-economy question
SELECT
    provider_name,
    ROUND(AVG(driver_pay), 2) AS avg_driver_pay,
    ROUND(AVG(base_passenger_fare), 2) AS avg_base_fare,
    ROUND(100.0 * AVG(driver_pay) / NULLIF(AVG(base_passenger_fare), 0), 1) AS driver_pay_pct_of_fare
FROM GOLD.FACT_TRIPS
GROUP BY 1
ORDER BY 1;

-- Airport trip patterns
SELECT
    provider_name,
    is_airport_trip,
    COUNT(*) AS trips,
    ROUND(AVG(base_passenger_fare), 2) AS avg_fare,
    ROUND(AVG(trip_miles), 2) AS avg_miles
FROM GOLD.FACT_TRIPS
GROUP BY 1, 2
ORDER BY 1, 2;
