{{
    config(
        tags=['weekly'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    ) 
}}

-- * File: qsr_user_behavior.sql
-- * Author: Hao Zhang
-- * Purpose:
--   Generates Quick Service Restaurant (QSR) features for users, 
--   including store counts near users and visitation behaviors during specific time periods.

-- * Step Overview:
--   - Step 1: Filter user activity data within the last 90 days.
--   - Step 2: Aggregate QSR store counts by zip code from multiple data sources.
--   - Step 3: Identify unique user zip codes for analysis.
--   - Step 4: Calculate QSR counts near user zip codes.
--   - Step 5: Aggregate QSR visitation counts by time segments (breakfast, lunch, etc.).
--   - Step 6: Combine and output all QSR-related features.
--
--  * Vars:
--      @param qsr_feature_look_back_window: The number of days to look back

WITH activity_filtered AS (
-- Step 1: Filter user activity data within the last 90 days
    SELECT DISTINCT username, zipcode AS zip_code, created_at_local
    FROM {{ ref('persona_user_activity_last_90_days') }}
    WHERE
        (created_date BETWEEN {{ var('ds') }}::DATE - INTERVAL '{{ var('qsr_feature_look_back_window', 90) }} DAYS'
            AND {{ var('ds') }}::DATE)
        AND (username IN (SELECT u.username FROM {{ ref('persona_universe') }} AS u))
),

-- Step 2: Aggregate QSR store counts by zip code
zip_qsr_count AS (
    SELECT zip_code, COUNT(store_id) AS store_count
    FROM (
        SELECT postal_code AS zip_code, store_id
        FROM {{ source('support', 'location_businesses_tacobell') }}

        UNION ALL SELECT postal_code AS zip_code, store_id
        FROM {{ source('support', 'location_businesses_mcdonalds') }}

        UNION ALL SELECT postal_code AS zip_code, store_id
        FROM {{ source('support', 'location_businesses_dunkin') }}

        UNION ALL SELECT postalcode AS zip_code, id AS store_id
        FROM {{ source('support', 'location_businesses_starbucks') }}

        UNION ALL SELECT postalcode AS zip_code, store_id
        FROM {{ source('support', 'location_businesses_burgerking') }}

        UNION ALL SELECT postal AS zip_code, id AS store_id
        FROM {{ source('support', 'location_businesses_wendys') }}

        UNION ALL SELECT TO_CHAR(zip) AS zip_code, TO_CHAR(id) AS store_id
        FROM {{ source('support', 'location_businesses_jackinthebox') }}

        UNION ALL SELECT zip AS zip_code, id AS store_id
        FROM {{ source('support', 'location_businesses_inandout') }}

        UNION ALL SELECT zip AS zip_code, store_id
        FROM {{ source('support', 'location_businesses_pizzahut') }}
        WHERE store_id <> 'HISTORY'
    )
    GROUP BY 1

),

-- Step 4: Calculate QSR counts near user zip codes
user_visited_qsr_cnt AS (
    SELECT u.username, SUM(COALESCE(z.store_count, 0)) AS qsr_near_zip
    FROM (
        SELECT DISTINCT username, zip_code
        FROM {{ ref('persona_general_location') }}
    ) AS u
    LEFT JOIN zip_qsr_count AS z ON (u.zip_code = z.zip_code)
    GROUP BY 1
),

zip_user_store_cnt AS (
    SELECT a.zip_code, a.user_count, b.store_count
    FROM 
    (
        SELECT zip_code, COUNT(DISTINCT username) user_count
        FROM {{ ref('persona_general_location') }}
        GROUP BY 1
    ) AS a
    LEFT JOIN zip_qsr_count AS b ON (a.zip_code = b.zip_code)
),

-- Step 5: Aggregate QSR visitation counts by time segments
qsr_store_count_agg AS (
    SELECT
        username,
        COUNT(DISTINCT zip_code) AS zips_visited,
        SUM(
            CASE WHEN HOUR(created_at_local) BETWEEN 6 AND 11 THEN COALESCE(store_count, 0) END
        ) AS breakfast_store_count,
        SUM(
            CASE WHEN HOUR(created_at_local) BETWEEN 6 AND 11 THEN COALESCE(user_count, 0) END
        ) AS breakfast_user_count,
        SUM(
            CASE WHEN HOUR(created_at_local) BETWEEN 11 AND 17 THEN COALESCE(store_count, 0) END
        ) AS lunch_store_count,
        SUM(
            CASE WHEN HOUR(created_at_local) BETWEEN 11 AND 17 THEN COALESCE(user_count, 0) END
        ) AS lunch_user_count,
        SUM(
            CASE WHEN HOUR(created_at_local) BETWEEN 17 AND 23 THEN COALESCE(store_count, 0) END
        ) AS dinner_store_count,
        SUM(
            CASE WHEN HOUR(created_at_local) BETWEEN 17 AND 23 THEN COALESCE(user_count, 0) END
        ) AS dinner_user_count,
        SUM(
            CASE WHEN HOUR(created_at_local) >= 23
                OR HOUR(created_at_local) BETWEEN 0 AND 6 THEN COALESCE(store_count, 0) END
        ) AS otherhours_store_count,
        SUM(
            CASE WHEN HOUR(created_at_local) >= 23
                OR HOUR(created_at_local) BETWEEN 0 AND 6 THEN COALESCE(user_count, 0) END
        ) AS otherhours_user_count
    FROM (
        SELECT
            a.username as username,
            a.zip_code,
            created_at_local,
            b.user_count,
            COALESCE(b.user_count, 0) * COALESCE(b.store_count, 0) AS store_count
    FROM activity_filtered AS a
        LEFT JOIN zip_user_store_cnt AS b ON (a.zip_code = b.zip_code)
    )
    GROUP BY 1
),


qsr_store_count AS (
    SELECT
        username,
        zips_visited,
        CASE 
            WHEN breakfast_user_count > 0 and breakfast_store_count > 0 THEN breakfast_store_count / breakfast_user_count
            ELSE 0 
        END AS breakfast_qsr_count,
        CASE 
            WHEN lunch_user_count > 0 THEN lunch_store_count / lunch_user_count
            ELSE 0 
        END AS lunch_qsr_count,
        CASE 
            WHEN dinner_user_count > 0 THEN dinner_store_count / dinner_user_count
            ELSE 0 
        END AS dinner_qsr_count,
        CASE 
            WHEN otherhours_user_count > 0 THEN otherhours_store_count / otherhours_user_count
            ELSE 0 
        END AS otherhours_qsr_count
    FROM qsr_store_count_agg
)

-- Step 6: Combine all QSR-related features
SELECT
    a.username,
    b.qsr_near_zip,
    a.zips_visited,
    a.breakfast_qsr_count,
    a.lunch_qsr_count,
    a.dinner_qsr_count,
    a.otherhours_qsr_count
FROM qsr_store_count AS a
LEFT JOIN user_visited_qsr_cnt AS b ON (a.username = b.username)
