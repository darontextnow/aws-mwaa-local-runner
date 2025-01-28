{{
    config(
        tags=['weekly'],
        materialized='table',
        snowflake_warehouse='PROD_WH_SMALL'
    ) 
}}
-- * File: persona_universe.sql
-- * Author: Hao Zhang
-- * Purpose:
--   Identifies and filters active users within the last X days who meet specific criteria,
--   including session activity and account status.

-- * Step Overview:
-- Step 1: Fetch unique users and determine account status.
-- Step 2: Retrieve the latest country codes for each user.
-- Step 3: Identify users active in the past X days based on session data.
-- Step 4: Map unique user IDs to usernames.
-- Step 5: Filter users based on criteria (enabled accounts, US-based, sufficient sessions).
-- Step 6: Combie

--  * Vars:
--      @param universe_look_back_window: this defines the number of days to look back
--          ! we set this to 7 days, because this is a weekly job

WITH stg_unique_users AS (
-- Step 1: Fetch unique users and determine account status
    SELECT
        username,
        CASE
            WHEN account_status LIKE 'HARD_DISABLED%' OR account_status LIKE '%DISABLED%'
                THEN 'DISABLED'
            ELSE 'ENABLED'
        END AS is_disabled
    FROM {{ source('core', 'users') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY username ORDER BY timestamp DESC) = 1
),

-- Step 2: Retrieve the latest country codes for each user
stg_latest_country_codes AS (
    SELECT username, country_code
    FROM {{ ref('registrations_1') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY username ORDER BY created_at DESC) = 1
),

-- Step 3: Identify users active in the past X days
stg_partyplanner_active_sessions AS (
    SELECT username
    FROM {{ ref('persona_user_activity_last_90_days') }}
    WHERE
        (created_date BETWEEN {{ var('ds') }}::DATE - INTERVAL '{{ var('universe_look_back_window', 7) }} DAYS'
            AND {{ var('ds') }}::DATE)
        AND (time_in_session > 0)
        AND (country_code = 'US')
        AND (LENGTH(username) > 0)
    GROUP BY 1
    HAVING (COUNT(*) > 1)
),

-- Step 4: Map unique user IDs to usernames
stg_unique_mappings AS (
    SELECT
        username,
        user_id_hex,
        created_at
    FROM {{ ref('analytics_users') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY username ORDER BY created_at DESC) = 1
),

-- Step 5: Filter users based on criteria
filtered_user_list AS (
    SELECT DISTINCT
        udm.username,
        uu.is_disabled,
        ucc.country_code,
        CASE
            WHEN pp.username IS NOT NULL THEN 'SUFFICIENT_SESSIONS'
            ELSE 'INSUFFICIENT_SESSIONS'
        END AS has_sessions
    FROM {{ source('dau', 'user_device_master') }} AS udm
    LEFT JOIN stg_unique_users AS uu ON (udm.username = uu.username)
    LEFT JOIN stg_latest_country_codes AS ucc ON (udm.username = ucc.username)
    LEFT JOIN stg_partyplanner_active_sessions AS pp ON (udm.username = pp.username)
    WHERE
        (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '{{ var('universe_look_back_window', 7) }} DAYS'
            AND {{ var('ds') }}::DATE)
        AND (uu.is_disabled = 'ENABLED')
        AND (ucc.country_code = 'US')
        AND (has_sessions = 'SUFFICIENT_SESSIONS')
)

-- Step 6: Combie
SELECT f.username, m.user_id_hex
FROM filtered_user_list AS f
LEFT JOIN stg_unique_mappings AS m ON (f.username = m.username)
