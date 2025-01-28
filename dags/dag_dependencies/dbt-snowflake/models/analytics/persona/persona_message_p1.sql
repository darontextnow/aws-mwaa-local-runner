{{
    config(
        tags=['weekly'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    ) 
}}

-- * File: message_behavior_features.sql
-- * Author: Hao Zhang
-- * Purpose:
--   This query generates messaging behavior features for users, including outbound 
--   and inbound message counts, time-of-day breakdowns, and MMS-specific metrics 
--   over the past 90 days.
--
-- * Step Overview:
--   - Step 1: Extract user location data with geospatial resolution (H3 indexing).
--   - Step 2: Enrich location data with timezone information.
--   - Step 3: Filter messages within the last 90 days.
--   - Step 4: Identify and classify outbound messages.
--   - Step 5: Aggregate outbound message counts by user.
--   - Step 6: Count outbound MMS messages.
--   - Step 7: Identify and classify inbound messages.
--   - Step 8: Aggregate inbound message counts by user.
--   - Step 9: Combine all aggregated features for modeling or analysis.
--
--  * Vars:
--      @param message_feature_look_back_window: The number of days to look back

-- Step 1: Extract user location data with H3 geospatial resolution
WITH user_locations AS (
    SELECT
        u.username,
        u.user_id_hex,
        l.lonlag_coords,
        ST_X(l.lonlag_coords)::DECIMAL(12, 9) AS longitude,
        ST_Y(l.lonlag_coords)::DECIMAL(11, 9) AS latitude,
        H3_LATLNG_TO_CELL(
            ST_Y(l.lonlag_coords),
            ST_X(l.lonlag_coords),
            2
        ) AS h3_res2
    FROM {{ ref('persona_universe') }} AS u
    LEFT JOIN {{ ref('location_user_location') }} AS l ON
        (u.username = l.username)
        AND ((ST_X(l.lonlag_coords) <> 0) OR (ST_Y(l.lonlag_coords) <> 0))
),

-- Step 2: Enrich location data with timezone information
locations_with_timezone AS (
    SELECT
        loc.username,
        loc.user_id_hex,
        loc.longitude,
        loc.latitude,
        loc.h3_res2,
        tz.tzid as timezone_code
    FROM user_locations AS loc
    LEFT JOIN {{ ref('location_timezone_boundary_h3') }} AS tz ON
        (tz.h3_res2_cell = loc.h3_res2)
        AND (CASE WHEN tz.tzid_count_in_cell > 1 THEN ST_WITHIN(loc.lonlag_coords, tz.geometry) ELSE TRUE END)
),

-- Step 3: Filter recent messages within the last 90 days
recent_messages AS (
    SELECT
        p.date_utc,
        p.created_at,
        loc.user_id_hex,
        loc.timezone_code,
        CASE WHEN loc.timezone_code IS NULL THEN 'NO_LOCAL_TZ'
            ELSE 'HAS_LOCAL_TZ' END AS timezone_status,
        CASE WHEN loc.timezone_code IS NULL THEN p.created_at
            ELSE CONVERT_TIMEZONE(loc.timezone_code, p.created_at) END AS created_at_local,
        p."payload.message_direction" AS message_direction,
        CASE 
            WHEN p."payload.content_type" = 'MESSAGE_TYPE_TEXT' THEN 'SMS'
            WHEN p."payload.content_type" IN ('MESSAGE_TYPE_IMAGE', 'MESSAGE_TYPE_VIDEO', 'MESSAGE_TYPE_AUDIO') THEN 'MMS'
            ELSE 'OTHER'
        END AS message_type
    FROM locations_with_timezone AS loc
    LEFT JOIN {{ source('party_planner_realtime', 'message_delivered') }} AS p ON (loc.user_id_hex = p.user_id_hex)
    WHERE
        (p.created_date BETWEEN {{ var('ds') }}::DATE - INTERVAL '{{ var('message_feature_look_back_window', 90) }} DAYS'
            AND {{ var('ds') }}::DATE)
        AND (p.user_id_hex <> '000-00-000-000000000')
        AND (p.instance_id LIKE 'MESS%')
),

-- Step 4: Identify outbound messages and classify by time segments
outbound_messages AS (
    SELECT
        user_id_hex,
        created_at_local,
        DAYOFWEEKISO(created_at_local) BETWEEN 1 AND 5 AS is_weekday,
        DAYOFWEEKISO(created_at_local) BETWEEN 6 AND 7 AS is_weekend,
        HOUR(created_at_local) BETWEEN 6 AND 11 AS is_morning,
        HOUR(created_at_local) BETWEEN 12 AND 17 AS is_afternoon,
        HOUR(created_at_local) BETWEEN 18 AND 23 AS is_evening,
        NOT(HOUR(created_at_local) BETWEEN 6 AND 23) AS is_other_time
    FROM recent_messages
    WHERE (message_direction = 'MESSAGE_DIRECTION_OUTBOUND')
),

-- Step 5: Aggregate outbound messages by user
aggregated_outbound_messages AS (
    SELECT
        user_id_hex,
        COUNT_IF(is_weekday) AS outbound_messages_weekday_90d,
        COUNT_IF(is_weekend) AS outbound_messages_weekend_90d,
        COUNT_IF(is_morning) AS outbound_messages_morning_90d,
        COUNT_IF(is_afternoon) AS outbound_messages_afternoon_90d,
        COUNT_IF(is_evening) AS outbound_messages_evening_90d,
        COUNT_IF(is_other_time) AS outbound_messages_other_time_90d
    FROM outbound_messages
    GROUP BY 1
),

-- Step 6: Count outbound MMS messages
outbound_mms_messages AS (
    SELECT user_id_hex, COUNT(*) AS outbound_mms_90d
    FROM recent_messages
    WHERE
        (message_direction = 'MESSAGE_DIRECTION_OUTBOUND')
        AND (message_type = 'MMS')
    GROUP BY 1
),

-- Step 7: Identify inbound messages and classify by time segments
inbound_messages AS (
    SELECT
        user_id_hex,
        created_at_local,
        DAYOFWEEKISO(created_at_local) BETWEEN 1 AND 5 AS is_weekday,
        DAYOFWEEKISO(created_at_local) BETWEEN 6 AND 7 AS is_weekend,
        HOUR(created_at_local) BETWEEN 6 AND 11 AS is_morning,
        HOUR(created_at_local) BETWEEN 12 AND 17 AS is_afternoon,
        HOUR(created_at_local) BETWEEN 18 AND 23 AS is_evening,
        NOT(HOUR(created_at_local) BETWEEN 6 AND 23) AS is_other_time
    FROM recent_messages
    WHERE (message_direction = 'MESSAGE_DIRECTION_INBOUND')
),

-- Step 8: Aggregate inbound message counts by user.
aggregated_inbound_messages as (
    SELECT
        user_id_hex,
        COUNT_IF(is_weekday) AS inbound_messages_weekday_90d,
        COUNT_IF(is_weekend) AS inbound_messages_weekend_90d,
        COUNT_IF(is_morning) AS inbound_messages_morning_90d,
        COUNT_IF(is_afternoon) AS inbound_messages_afternoon_90d,
        COUNT_IF(is_evening) AS inbound_messages_evening_90d,
        COUNT_IF(is_other_time) AS inbound_messages_other_time_90d
    FROM inbound_messages
    GROUP BY 1
)

-- Step 9: Combine all aggregated features for modeling or analysis.
SELECT
    COALESCE(o.user_id_hex, i.user_id_hex) AS user_id_hex,
    COALESCE(o.outbound_messages_weekday_90d, 0) AS outbound_messages_weekday_90d,
    COALESCE(o.outbound_messages_weekend_90d, 0) AS outbound_messages_weekend_90d,
    COALESCE(o.outbound_messages_morning_90d, 0) AS outbound_messages_morning_90d,
    COALESCE(o.outbound_messages_afternoon_90d, 0) AS outbound_messages_afternoon_90d,
    COALESCE(o.outbound_messages_evening_90d, 0) AS outbound_messages_evening_90d,
    COALESCE(o.outbound_messages_other_time_90d, 0) AS outbound_messages_other_time_90d,
    COALESCE(m.outbound_mms_90d, 0) AS outbound_mms_90d,
    COALESCE(i.inbound_messages_weekday_90d, 0) AS inbound_messages_weekday_90d,
    COALESCE(i.inbound_messages_weekend_90d, 0) AS inbound_messages_weekend_90d,
    COALESCE(i.inbound_messages_morning_90d, 0) AS inbound_messages_morning_90d,
    COALESCE(i.inbound_messages_afternoon_90d, 0) AS inbound_messages_afternoon_90d,
    COALESCE(i.inbound_messages_evening_90d, 0) AS inbound_messages_evening_90d,
    COALESCE(i.inbound_messages_other_time_90d, 0) AS inbound_messages_other_time_90d
FROM aggregated_outbound_messages AS o
FULL OUTER JOIN outbound_mms_messages AS m ON (o.user_id_hex = m.user_id_hex)
FULL OUTER JOIN aggregated_inbound_messages AS i ON (o.user_id_hex = i.user_id_hex)
