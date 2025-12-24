-- Name: Jimma Shanko
-- Course: SEIS 732 Data Warehousing and BI
-- Instructor: Nathan Crawford
-- PROJECT: The impact of weather on Business based on "Global Weather & Climate Data for BI" and "uszips.csv"
--- Due Date: 11/20/2024
     
/////////////////////////////////////////////////////////////

            -- Worksheet1_curation.sql
            
//////////////////////////////////////////////////////////////
// How many of my deliveries will be delayed due to snowfall?
/*
When it snows in excess of six inches per day, my company experiences delivery delays. How many of my deliveries were impacted during the third week of January for the previous year?
*/

//create my own database and schema
USE ROLE TRAINING_ROLE;
CREATE WAREHOUSE IF NOT EXISTS EAGLE_WH;
USE WAREHOUSE EAGLE_WH;
CREATE DATABASE IF NOT EXISTS EAGLE_DB;
USE EAGLE_DB.PUBLIC;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW_WEATHER;       -- Raw/ingested data
CREATE SCHEMA IF NOT EXISTS CUR_WEATHER;       -- Curation layer
CREATE SCHEMA IF NOT EXISTS AGG_WEATHER;       -- Aggregation layer


-- HISTORY_DAY
CREATE OR REPLACE VIEW RAW_WEATHER.HISTORY_DAY AS
SELECT * FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.HISTORY_DAY;

-- FORECAST_DAY
CREATE OR REPLACE VIEW RAW_WEATHER.FORECAST_DAY AS
SELECT * FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.FORECAST_DAY;

-- CLIMATOLOGY_DAY
CREATE OR REPLACE VIEW RAW_WEATHER.CLIMATOLOGY_DAY AS
SELECT * FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.CLIMATOLOGY_DAY;

USE SCHEMA CUR_WEATHER;

-------------------------------------------------------------------
CREATE OR REPLACE TABLE CUR_WEATHER.ZIP_LOOKUP (
    ZIP STRING,
    CITY STRING,
    STATE STRING
);


----------------------------------------
-- RAW LAYER
----------------------------------------
CREATE OR REPLACE VIEW RAW_WEATHER.HISTORY_DAY AS
SELECT * 
FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.HISTORY_DAY;

CREATE OR REPLACE VIEW RAW_WEATHER.FORECAST_DAY AS
SELECT *
FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.FORECAST_DAY;

CREATE OR REPLACE VIEW RAW_WEATHER.CLIMATOLOGY_DAY AS
SELECT *
FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.CLIMATOLOGY_DAY;

----------------------------------------
-- TAG (semantic policy)
----------------------------------------
CREATE OR REPLACE TAG semantic_policy;

----------------------------------------
-- STEP 1: CLEAN NULLS + INGEST TIMESTAMP
----------------------------------------
CREATE OR REPLACE VIEW CUR_WEATHER.CUR_HISTORY_CLEANED AS
SELECT
    *,
    CURRENT_TIMESTAMP() AS ingested_at,
    IFF(tot_precipitation_in IS NULL, 0, tot_precipitation_in) AS tot_precip_clean,
    IFF(avg_temperature_air_2m_f IS NULL, 0, avg_temperature_air_2m_f) AS avg_temp_clean
FROM RAW_WEATHER.HISTORY_DAY;

ALTER VIEW CUR_WEATHER.CUR_HISTORY_CLEANED 
SET TAG semantic_policy = 'project';

----------------------------------------
-- STEP 2: FLAG HEAVY SNOW (>6 IN)
----------------------------------------
CREATE OR REPLACE VIEW CUR_WEATHER.CUR_HISTORY_SNOW_FLAG AS
SELECT
    *,
    IFF(tot_snowfall_in > 6, 'Yes', 'No') AS heavy_snow_flag
FROM CUR_WEATHER.CUR_HISTORY_CLEANED;

ALTER VIEW CUR_WEATHER.CUR_HISTORY_SNOW_FLAG 
SET TAG semantic_policy = 'project';

----------------------------------------
-- STEP 3: FLAG HEAVY RAIN (>1 IN)
----------------------------------------
CREATE OR REPLACE VIEW CUR_WEATHER.CUR_HISTORY_RAIN_FLAG AS
SELECT
    *,
    IFF(tot_precip_clean > 1, 'Yes', 'No') AS heavy_rain_flag
FROM CUR_WEATHER.CUR_HISTORY_SNOW_FLAG;

ALTER VIEW CUR_WEATHER.CUR_HISTORY_RAIN_FLAG 
SET TAG semantic_policy = 'project';

----------------------------------------
-- STEP 4: TEMPERATURE CATEGORY
----------------------------------------
CREATE OR REPLACE VIEW CUR_WEATHER.CUR_HISTORY_TEMP_CAT AS
SELECT
    *,
    CASE 
        WHEN avg_temp_clean >= 80 THEN 'Hot'
        WHEN avg_temp_clean >= 60 THEN 'Warm'
        ELSE 'Cold'
    END AS temp_category
FROM CUR_WEATHER.CUR_HISTORY_RAIN_FLAG;

ALTER VIEW CUR_WEATHER.CUR_HISTORY_TEMP_CAT 
SET TAG semantic_policy = 'project';

----------------------------------------
-- STEP 5: FINAL CURATED TABLE
----------------------------------------
CREATE OR REPLACE TABLE CUR_WEATHER.CUR_HISTORY_FINAL AS
SELECT
    *,
    YEAR(date_valid_std)  AS year,
    MONTH(date_valid_std) AS month,
    DAY(date_valid_std)   AS day
FROM CUR_WEATHER.CUR_HISTORY_TEMP_CAT;

ALTER TABLE CUR_WEATHER.CUR_HISTORY_FINAL 
SET TAG semantic_policy = 'project';

WITH previous_year_dates AS (
    SELECT
        YEAR(CURRENT_DATE()) - 1 AS prev_year,
        -- Third week of January = Jan 15–21
        DATE_FROM_PARTS(YEAR(CURRENT_DATE()) - 1, 1, 15) AS start_dt,
        DATE_FROM_PARTS(YEAR(CURRENT_DATE()) - 1, 1, 21) AS end_dt
)
SELECT
    h.postal_code,
    IFF(z.city IS NULL, 'Unknown', z.city) AS city,
    IFF(z.state_name IS NULL, 'Unknown', z.state_name) AS state_name,
    CASE
        WHEN h.country IN ('US') THEN 'United States'
        ELSE h.country
    END AS country,    
    h.date_valid_std,
    h.tot_snowfall_in,
    h.heavy_snow_flag
FROM CUR_WEATHER.CUR_HISTORY_FINAL h
JOIN previous_year_dates d
    ON h.date_valid_std BETWEEN d.start_dt AND d.end_dt
LEFT JOIN RAW_WEATHER.ZIP_RAW z
    ON h.postal_code = z.zip
WHERE h.heavy_snow_flag = 'Yes'
ORDER BY h.postal_code, h.date_valid_std, h.tot_snowfall_in;


/////////////////////////////////////////////
---------------------------------------------
-- Worksheet #2 – Stored Procedure
-- Weekly transformations + delivery delay flag
---------------------------------------------
-- Calculating weekly average temprature and total snowfalls per Zip code for forecast data.
//create my own database and schema

CREATE OR REPLACE PROCEDURE SP_HISTORY_TRANSFORM()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

    CREATE OR REPLACE TABLE CUR_WEATHER.CUR_HISTORY_DELIVERY AS
    SELECT
        h.postal_code,
        IFF(z.city IS NULL, 'Unknown', z.city) AS city,
        IFF(z.state_name IS NULL, 'Unknown', z.state_name) AS state_name,
        'United States' AS country,        
        h.date_valid_std,
        h.tot_snowfall_in,
        h.avg_temperature_air_2m_f,

        IFF(h.tot_snowfall_in > 6, 1, 0) AS delivery_delay_flag,
        CEIL(DAY(h.date_valid_std) / 7) AS week_of_month,

        CASE
            WHEN h.avg_temperature_air_2m_f >= 85 THEN 'Very Hot'
            WHEN h.avg_temperature_air_2m_f >= 70 THEN 'Warm'
            WHEN h.avg_temperature_air_2m_f >= 50 THEN 'Mild'
            WHEN h.avg_temperature_air_2m_f >= 32 THEN 'Cold'
            ELSE 'Freezing'
        END AS feels_like_category

    FROM CUR_WEATHER.CUR_HISTORY_FINAL h
    LEFT JOIN RAW_WEATHER.ZIP_RAW z
        ON h.postal_code = z.zip
    WHERE h.country = 'US';  -- Only United States

    RETURN 'SUCCESS: CUR_HISTORY_DELIVERY created for U.S. postal codes.';
END;
$$;

-- Call the procedure
CALL SP_HISTORY_TRANSFORM();

-- Verify results
SELECT * FROM CUR_WEATHER.CUR_HISTORY_DELIVERY LIMIT 60000;
----------------------------------------

-- Worksheet3_Aggregation.sql

-- -------------------------------
-- 1️ Average air temperature by country
-- -------------------------------

CREATE OR REPLACE TABLE AVG_AIR_TEMP_BY_COUNTRY AS
SELECT 
    DATE_VALID_STD,    
    POSTAL_CODE,
    COUNTRY,
    AVG(MAX_TEMPERATURE_AIR_2M_F) AS AVG_MAX_AIR_TEMP,
    AVG(MIN_TEMPERATURE_AIR_2M_F) AS AVG_MIN_AIR_TEMP
FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.HISTORY_DAY
WHERE COUNTRY = 'US'
GROUP BY DATE_VALID_STD, POSTAL_CODE, COUNTRY;


-- -------------------------------
-- 2️ Hottest day by country
-- -------------------------------

CREATE OR REPLACE TABLE HOTTEST_DAY_BY_COUNTRY AS
SELECT
  DATE_VALID_STD,    
  POSTAL_CODE,  
  COUNTRY,
  MAX(MAX_TEMPERATURE_AIR_2M_F) AS MAX_TEMP
FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.HISTORY_DAY
WHERE COUNTRY = 'US'
GROUP BY DATE_VALID_STD, POSTAL_CODE, COUNTRY;

-- -------------------------------
-- 3️ Coldest day by country
-- -------------------------------

CREATE OR REPLACE TABLE COLDEST_DAY_BY_COUNTRY AS
SELECT
  DATE_VALID_STD,    
  POSTAL_CODE, 
  COUNTRY,
  MIN(MIN_TEMPERATURE_AIR_2M_F) AS MIN_TEMP
FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.HISTORY_DAY
WHERE COUNTRY = 'US'
GROUP BY DATE_VALID_STD, POSTAL_CODE, COUNTRY;

-- -------------------------------
-- 4️ Count of forecast records per postal code
-- -------------------------------

CREATE OR REPLACE TABLE COUNT_FORECAST_BY_POSTAL AS
SELECT 
    POSTAL_CODE,
    COUNT(*) AS FORECAST_RECORDS
FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.FORECAST_DAY
WHERE COUNTRY = 'US'
GROUP BY POSTAL_CODE;

-- -------------------------------
-- 5️ Snow prediction by postal code
-- -------------------------------

CREATE OR REPLACE TABLE SNOW_PREDICTION_BY_POSTAL AS
SELECT
    POSTAL_CODE,
    COUNTRY,
    DATE_VALID_STD,
    AVG(TOT_SNOWFALL_IN) AS AVG_SNOWFALL_IN,
    SUM(TOT_SNOWFALL_IN) AS TOTAL_SNOWFALL_IN,
    CASE 
        WHEN AVG(TOT_SNOWFALL_IN) > 1 THEN 'HIGH CHANCE OF SNOW'
        WHEN AVG(TOT_SNOWFALL_IN) BETWEEN 0.1 AND 1 THEN 'POSSIBLE SNOW'
        ELSE 'LOW CHANCE OF SNOW'
    END AS SNOW_PREDICTION
FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.HISTORY_DAY
WHERE COUNTRY = 'US'
GROUP BY POSTAL_CODE, COUNTRY, DATE_VALID_STD;

-- -------------------------------
-- 6️ Count of delay days (heavy snow days)
-- -------------------------------

CREATE OR REPLACE TABLE AGG_WEATHER.DELAY_DAY_COUNT AS
SELECT 
    POSTAL_CODE,
    DATE_VALID_STD,
    COUNT_IF(heavy_snow_flag='Yes') AS TOTAL_DELAY_DAYS
FROM CUR_WEATHER.CUR_HISTORY_FINAL
WHERE COUNTRY = 'US'
GROUP BY POSTAL_CODE, DATE_VALID_STD;

-- ---------------------------------------------
-- 7️ Weather summary base table with city/state
-- ---------------------------------------------

-- 1. Create pre-aggregated temp tables
CREATE OR REPLACE TABLE AVG_TEMP AS
SELECT POSTAL_CODE, DATE_VALID_STD, COUNTRY,
       AVG(MAX_TEMPERATURE_AIR_2M_F) AS AVG_MAX_AIR_TEMP,
       AVG(MIN_TEMPERATURE_AIR_2M_F) AS AVG_MIN_AIR_TEMP
FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.HISTORY_DAY
WHERE COUNTRY = 'US'
GROUP BY POSTAL_CODE, DATE_VALID_STD, COUNTRY;

CREATE OR REPLACE TABLE HOTTEST AS
SELECT COUNTRY, MAX(MAX_TEMPERATURE_AIR_2M_F) AS MAX_TEMP
FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.HISTORY_DAY
WHERE COUNTRY = 'US'
GROUP BY COUNTRY;

CREATE OR REPLACE TABLE COLDEST AS
SELECT COUNTRY, MIN(MIN_TEMPERATURE_AIR_2M_F) AS MIN_TEMP
FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.HISTORY_DAY
WHERE COUNTRY = 'US'
GROUP BY COUNTRY;

-- 2. Then create the summary table using these pre-aggregated tables
CREATE OR REPLACE TABLE AGG_WEATHER.WEATHER_SUMMARY_BASE AS
SELECT 
    c.COUNTRY_NAME,
    s.POSTAL_CODE,
    IFF(z.CITY IS NULL, 'Unknown', z.CITY) AS CITY,
    IFF(z.STATE_NAME IS NULL, 'Unknown', z.STATE_NAME) AS STATE_NAME,
    s.DATE_VALID_STD,
    a.AVG_MAX_AIR_TEMP,
    a.AVG_MIN_AIR_TEMP,
    h.MAX_TEMP,
    l.MIN_TEMP,
    s.AVG_SNOWFALL_IN,
    s.TOTAL_SNOWFALL_IN,
    s.SNOW_PREDICTION,
    d.TOTAL_DELAY_DAYS
FROM SNOW_PREDICTION_BY_POSTAL s
JOIN COUNTRY_CODES c
    ON s.COUNTRY = c.COUNTRY_CODE
LEFT JOIN RAW_WEATHER.ZIP_RAW z
    ON s.POSTAL_CODE = z.ZIP
LEFT JOIN AVG_TEMP a
    ON s.POSTAL_CODE = a.POSTAL_CODE
   AND s.DATE_VALID_STD = a.DATE_VALID_STD
   AND s.COUNTRY = a.COUNTRY
LEFT JOIN HOTTEST h
    ON h.COUNTRY = s.COUNTRY
LEFT JOIN COLDEST l
    ON l.COUNTRY = s.COUNTRY
LEFT JOIN AGG_WEATHER.DELAY_DAY_COUNT d
    ON d.POSTAL_CODE = s.POSTAL_CODE
   AND d.DATE_VALID_STD = s.DATE_VALID_STD
WHERE s.COUNTRY = 'US';

-- -------------------------------
-- 8️ Materialized view
-- -------------------------------
CREATE OR REPLACE MATERIALIZED VIEW MV_WEATHER_SUMMARY AS
SELECT *
FROM AGG_WEATHER.WEATHER_SUMMARY_BASE;

-- Verify materialized view
SHOW MATERIALIZED VIEWS LIKE 'MV_WEATHER_SUMMARY';
SELECT * FROM MV_WEATHER_SUMMARY LIMIT 3000;
--------------------------------------------
-- worksheet4_Function.sql
-- -----------------------------------------
CREATE OR REPLACE FUNCTION FN_GET_US_WEATHER_SUMMARY()
RETURNS TABLE(
    POSTAL_CODE STRING,
    CITY STRING,
    STATE_NAME STRING,
    DATE_VALID_STD DATE,
    AVG_MAX_AIR_TEMP NUMBER,
    AVG_MIN_AIR_TEMP NUMBER,
    MAX_TEMP NUMBER,
    MIN_TEMP NUMBER,
    AVG_SNOWFALL_IN NUMBER,
    TOTAL_SNOWFALL_IN NUMBER,
    SNOW_PREDICTION STRING,
    TOTAL_DELAY_DAYS NUMBER
)
AS
$$
    SELECT 
        c.postal_code,
        COALESCE(z.city, 'Unknown') AS CITY,
        COALESCE(z.state_name, 'Unknown') AS STATE_NAME,
        c.date_valid_std,
        c.avg_temp_clean AS AVG_MAX_AIR_TEMP,
        c.avg_temp_clean AS AVG_MIN_AIR_TEMP,
        c.avg_temp_clean AS MAX_TEMP,
        c.avg_temp_clean AS MIN_TEMP,
        c.tot_snowfall_in AS AVG_SNOWFALL_IN,
        c.tot_snowfall_in AS TOTAL_SNOWFALL_IN,
        CASE 
            WHEN c.tot_snowfall_in > 1 THEN 'HIGH CHANCE OF SNOW'
            ELSE 'LOW CHANCE OF SNOW'
        END AS SNOW_PREDICTION,
        CASE 
            WHEN c.tot_snowfall_in > 6 THEN 1
            ELSE 0
        END AS TOTAL_DELAY_DAYS
    FROM EAGLE_DB.CUR_WEATHER.CUR_HISTORY_FINAL c
    LEFT JOIN EAGLE_DB.RAW_WEATHER.ZIP_RAW z
        ON c.postal_code = z.zip
    WHERE c.country = 'US'
$$;

-- Example call
SELECT *
FROM TABLE(FN_GET_US_WEATHER_SUMMARY())
LIMIT 60000;
-----------------------------

-- worksheet6_tasks.sql
---------------------------------------


-- Creating a weekly task that calls the stored procedure
CREATE OR REPLACE TASK TASK_REFRESH_HISTORY
    WAREHOUSE = EAGLE_WH
    SCHEDULE = 'USING CRON 0 4 * * SUN UTC'
AS
    CALL CUR_WEATHER.SP_HISTORY_TRANSFORM();

EXECUTE TASK TASK_REFRESH_HISTORY;

-- verification
SHOW TASKS LIKE 'TASK_REFRESH_HISTORY';

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_REFRESH_HISTORY',
    RESULT_LIMIT => 5
));
--suspend it after run to avoid charges
ALTER TASK TASK_REFRESH_HISTORY SUSPEND;


//////////////////////////////
// Worksheet7: Summary
//////////////////////////////

/*
A. Dataset Name and Description
I selected the Weather Source "GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI" from the snowflake Markerplace. This dataset provides high-resolution historical, present, and forecasted weather data at global coverage, including variables such temperature, precipitation, snowfall, and postal-code level weather metrics. It is commonly used for analyticc involving demand forecasting, losgistics optimizaiton, economic impact modeding, and weather-driven business decisions.

B. Naming Convention and Schemas
- RAW_* - Landing zone for unchanged sources tables from the Marketplace.
- CUR_* - Curation layer where data is cleaned, standardized, and enriched.
- AGG_* - Aggregation layer containing summarized, analytical tables and materialized views.
* All shemas are located inside my database: EAGLE_DB.RAW_WEATHER, EAGLE_DB.CUR_WEATHER, and EAGLE_DB.AGG_WEATHER

C. Mini data Catalog
The curation layer includes field such as DATE_STD, which converts DATE_VALID_STD into a standardized DATE format, and COUNTRY_NAME,  which uses a DECODE mapping to replace ISO contry codes with readable names like United States, India, and Germany. In the aggregation layer, field such as AVG_MAX_AIR_TEMP, AVG_MIN_AIR_TEMP, AVG_SNOWFALL_IN, and TOTAL_SNOWFALL_IN were created using standard aggregation functions (AVG and SUM) to compute monthly temperature, precipitation, and snowfall metrics. Additional field MAX_TEMP and MIN_TEMP capture the hottest and coldest recorded temperatures, while TOTAL_DELAY_DAYS counts days with severe weather conditions that may cause operational or delivery delays.
*/

///////////////////////////////////////////////////////////////
--------------------------------------------------
-- DASHBOARD
--------------------------------------------------
-- 1. AVG_TEMP_BY_STATE  — Data as of 2024-10-12
CREATE OR REPLACE TABLE AGG_WEATHER.AVG_TEMP_BY_STATE AS
SELECT 
    z.STATE_NAME,
    AVG(h.MAX_TEMPERATURE_AIR_2M_F) AS AVG_MAX_AIR_TEMP,
    AVG(h.MIN_TEMPERATURE_AIR_2M_F) AS AVG_MIN_AIR_TEMP
FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.HISTORY_DAY h
JOIN EAGLE_DB.RAW_WEATHER.ZIP_RAW z
    ON h.POSTAL_CODE = z.ZIP
WHERE z.STATE_NAME IS NOT NULL
GROUP BY z.STATE_NAME;
SELECT
    STATE_NAME,
    AVG_MAX_AIR_TEMP
FROM AGG_WEATHER.AVG_TEMP_BY_STATE
WHERE STATE_NAME IN ('Minnesota','Florida','Washington','New York','Texas')
ORDER BY AVG_MAX_AIR_TEMP DESC;
/////////////////////////////////////////
---------------------------------------------
-- 2. Snowfall Comparison by State
---------------------------------------------
CREATE OR REPLACE TABLE 
AGG_WEATHER.AVG_SNOW_BY_STATE AS
SELECT 
    z.STATE_NAME,
    AVG(m.AVG_SNOWFALL_IN) AS AVG_SNOWFALL_IN,
    SUM(m.TOTAL_SNOWFALL_IN) AS TOTAL_SNOWFALL_IN
FROM AGG_WEATHER.WEATHER_SUMMARY_BASE m
JOIN EAGLE_DB.RAW_WEATHER.ZIP_RAW z
    ON LPAD(z.ZIP::TEXT, 5, '0') = m.POSTAL_CODE
WHERE z.STATE_NAME IN ('Minnesota','Florida','Washington','New York','Texas')
GROUP BY z.STATE_NAME;

SELECT
    STATE_NAME,
    AVG_SNOWFALL_IN,
    TOTAL_SNOWFALL_IN
FROM AGG_WEATHER.AVG_SNOW_BY_STATE
ORDER BY AVG_SNOWFALL_IN DESC;
/////////////////////////////////////////

-----------------------------------------
-- 3.TOTAL_SNOWFALL_INCH by STATE
-----------------------------------------
CREATE OR REPLACE TABLE AGG_WEATHER.TOT_SNOWFALL_BY_STATE AS
SELECT
    z.STATE_NAME,
    SUM(h.TOT_SNOWFALL_IN) AS TOT_SNOWFALL_IN
FROM GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI.STANDARD_TILE.HISTORY_DAY h
JOIN EAGLE_DB.RAW_WEATHER.ZIP_RAW z
    ON h.POSTAL_CODE = z.ZIP
WHERE z.STATE_NAME IS NOT NULL
GROUP BY z.STATE_NAME;
SELECT
    STATE_NAME,
    TOT_SNOWFALL_IN
FROM AGG_WEATHER.TOT_SNOWFALL_BY_STATE
WHERE STATE_NAME IN ('Minnesota','Florida','Washington','New York','Texas')
ORDER BY TOT_SNOWFALL_IN DESC;
///////////////////////////////////////

--------------------------------------------
-- 4. DELIVERY DELAY BY STATE
--------------------------------------------
SELECT
    c.postal_code AS POSTAL_CODE,
    COALESCE(z.city, 'Unknown') AS CITY,
    COALESCE(z.state_name, 'Unknown') AS STATE_NAME,
    SUM(CASE 
            WHEN c.tot_snowfall_in > 6 THEN 1
            ELSE 0
        END) AS TOTAL_DELAY
FROM EAGLE_DB.CUR_WEATHER.CUR_HISTORY_FINAL c
LEFT JOIN EAGLE_DB.RAW_WEATHER.ZIP_RAW z
    ON c.postal_code = z.zip
WHERE c.country = 'US'
GROUP BY c.postal_code, CITY, STATE_NAME
ORDER BY TOTAL_DELAY DESC
LIMIT 5;


//////////////////////////////////////////