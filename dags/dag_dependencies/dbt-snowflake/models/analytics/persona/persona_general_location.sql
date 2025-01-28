{{
    config(
        tags=['weekly'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    ) 
}}

-- * File: marketing_personas_general_location.sql
-- * Author: Hao Zhang
-- * Purpose:
--   This query enriches user location data by combining IP-based geolocation,
--   user-reported location, and additional mappings for ZIP codes, DMAs, 
--   timezones, and coordinates.
--
-- * Step Overview:
--   - Step 1: Extract user location details from IP data (p2_user_loc).
--   - Step 2: Extract user-reported location details (p1_user_loc).
--   - Step 3: Combine the IP-based and user-reported locations (matched).
--   - Step 4: Enrich the combined location data with timezone and coordinate details.

--
--  * Vars:
--      @param location_feature_look_back_window: The number of days to look back


WITH p2_user_loc AS (
-- Step 1: Extract user location from IP data
    SELECT
        username,
        zcta5.zcta5 AS zip_code,
        location_point
    FROM (
        SELECT
            a.username,
            ANY_VALUE(ST_POINT(ip.longitude, ip.latitude)) AS location_point,
            H3_POINT_TO_CELL(ANY_VALUE(ST_POINT(ip.longitude, ip.latitude)), 6) AS h3_res6_cell
        FROM {{ ref('persona_universe') }} AS a
        INNER JOIN {{ ref('user_ip_master') }} AS ip ON (a.username = ip.username)
        WHERE (date_utc >= {{ var('ds') }}::DATE - INTERVAL '{{ var('location_feature_look_back_window', 30) }} DAYS')
        GROUP BY 1
    ) AS a
    JOIN {{ ref('location_zcta5_boundary_h3') }} AS zcta5 ON
        (ST_WITHIN(a.location_point, zcta5.geometry))
        AND (a.h3_res6_cell = zcta5.h3_res6_cell)
),

-- Step 2: Extract user-reported location details
p1_user_loc AS (
    SELECT
        username,
        zip_code,
        zip_code_source,
        dma_name,
        lonlag_coords as location_point,
        lonlag_coords_source as location_point_source
    FROM {{ ref('location_user_location') }}
    LEFT JOIN {{ source('support', 'zip_dma_mapping_2020') }} ON (zip_code = zip)
),

-- Step 3: Combine IP-based and user-reported locations
matched AS (
    SELECT
        mpu.username,
        COALESCE(p1.zip_code, p2.zip_code) AS zip_code,
        COALESCE(p1.zip_code_source, 'user_ip_master') AS zip_code_source,
        COALESCE(p1.dma_name, 'Unknown') AS dma_name,
        COALESCE(p1.location_point, p2.location_point) AS location_point
    FROM {{ ref('persona_universe') }} AS mpu
    LEFT JOIN p1_user_loc AS p1 ON (mpu.username = p1.username)
    LEFT JOIN p2_user_loc AS p2 ON (mpu.username = p2.username)
)

-- Step 4: Enrich combined data with timezone and coordinates
SELECT
    matched.*,
    COALESCE(m.timezone, tz.tzid) AS timezone,
    ST_Y(matched.location_point) AS zip_latitude,
    ST_X(matched.location_point) AS zip_longitude
FROM matched
LEFT JOIN {{ source('support', 'us_zip_code_mapping') }} AS m ON (matched.zip_code = LPAD(m.zip, 5, '0'))
LEFT JOIN {{ ref('location_timezone_boundary_h3') }} AS tz ON
    (ST_WITHIN(matched.location_point, tz.geometry))
