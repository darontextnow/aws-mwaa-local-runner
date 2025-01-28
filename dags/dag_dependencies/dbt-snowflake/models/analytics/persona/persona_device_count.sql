{{
    config(
        tags=['weekly'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    ) 
}}

-- * File: persona_device_count.sql
-- * Author: Hao Zhang
-- * Purpose: 
--   This query generates user-level device features by aggregating device usage 
--   data, including total devices, device type counts, and the maximum number of 
--   concurrent devices in a 14-day window.
--
-- * Step Overview: 
--   - Step 1: Aggregate device counts by type for each user over the specified period.
--   - Step 2: Calculate the maximum number of concurrent devices used by each user
--     in any 14-day time window within the specified period.
--   - Step 3: Combine device type counts and max concurrent devices into the final output.
--  
--  * Vars:
--      @param device_feature_look_back_window: The number of days to look back

WITH device_count_by_type AS (
-- Step 1: Aggregate device counts by type
    SELECT
        a.username,  -- Unique identifier for each user
        COUNT(DISTINCT b.adid) AS num_devices_12mo, -- Total number of devices
        COUNT(DISTINCT CASE WHEN device_type = 'phone' THEN b.adid END) AS num_phones_12mo,  -- Count of phones
        COUNT(DISTINCT CASE WHEN device_type = 'tablet' THEN b.adid END) AS num_tablets_12mo,  -- Count of tablets
        COUNT(DISTINCT CASE WHEN device_type NOT IN ('phone', 'tablet') THEN b.adid END ) AS num_other_devices_12mo  -- Count of other device types
    FROM {{ ref('persona_universe') }} AS a
    JOIN {{ source('dau', 'user_device_master') }} AS b ON (a.username = b.username)
    JOIN {{ ref('installs_with_pi') }} AS c ON (b.adid = c.adid)
    WHERE
        (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '{{ var('device_feature_look_back_window', 30) }} DAYS' - INTERVAL '1 YEAR'
            AND {{ var('ds') }}::DATE)
    GROUP BY 1
),

-- Step 2: Calculate maximum concurrent devices in a 14-day window
curr_devices AS (
    SELECT username, MAX(num_devices_in_window) AS max_concurrent_devices
    FROM (
        SELECT username, num_devices_in_window
        FROM (
            SELECT
                TIME_SLICE(date_utc, 14, 'WEEK') AS window,
                a.username,
                COUNT(DISTINCT adid) AS num_devices_in_window
            FROM {{ ref('persona_universe') }} AS a
            JOIN {{ source('dau', 'user_device_master') }} AS b ON (a.username = b.username)
            WHERE
                (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '{{ var('device_feature_look_back_window', 30) }} DAYS' - INTERVAL '1 YEAR'
                    AND {{ var('ds') }}::DATE)
            GROUP BY 1, 2
        )
        GROUP BY 1, 2
        HAVING (COUNT(1) > 1)
    )
    GROUP BY 1
)

-- Step 3: Combine device type counts and max concurrent devices into final output
SELECT
    a.*,
    COALESCE(b.max_concurrent_devices, 1) AS max_concurrent_devices
FROM device_count_by_type AS a
LEFT JOIN curr_devices AS b ON (a.username = b.username)
