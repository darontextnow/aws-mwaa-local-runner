{{
    config(
        tags=['weekly'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    ) 
}}

-- * File: mms_message_counts.sql
-- * Author: Hao Zhang
-- * Purpose:
--   Extracts and aggregates MMS messaging data over the past 90 days, providing 
--   the total number of MMS messages sent by each user for analysis.

-- * Step Overview:
--   - Step 1: Extract message details, including type and metadata, from the `messagedelivered` table.
--   - Step 2: Aggregate MMS message counts by user.
--   - Step 3: Return MMS message counts for each user.

--  * Vars:
--      @param message_feature_look_back_window: The number of days to look back

WITH message_details AS (
-- Step 1: Extracts message details
    SELECT
        p.date_utc,
        p.created_at,
        u.user_id_hex,
        u.username,
        p."payload.message_direction" AS message_direction,
        CASE WHEN p."payload.content_type" = 'MESSAGE_TYPE_TEXT' THEN 'SMS'
             WHEN p."payload.content_type" IN ('MESSAGE_TYPE_IMAGE', 'MESSAGE_TYPE_VIDEO', 'MESSAGE_TYPE_AUDIO') THEN 'MMS'
             ELSE 'OTHER'
        END AS message_type,
        p."payload.target" AS destination_number,
        p."payload.target_locale.country_code" AS destination_country_code,
        p."payload.origin" AS origin_number,
        p."payload.origin_locale.country_code" AS origin_country_code,
        p."client_details.client_data.client_ip_address" AS client_ip_address
    FROM {{ ref('persona_universe') }} AS u
    LEFT JOIN {{ source('party_planner_realtime', 'message_delivered') }} AS p ON (u.user_id_hex = p.user_id_hex)
    WHERE
        (p.created_date BETWEEN {{ var('ds') }}::DATE - INTERVAL '{{ var('message_feature_look_back_window', 90) }} DAYS'
            AND {{ var('ds') }}::DATE)
        AND (p.user_id_hex <> '000-00-000-000000000')
        AND (p.instance_id LIKE 'MESS%')
),

-- Step 2: Aggregates MMS message counts by user
mms_messages AS (
    SELECT
        user_id_hex,
        username,
        COUNT(message_type) AS no_mms_sent
    FROM message_details
    WHERE (message_type = 'MMS')
    GROUP BY 1, 2
)

-- Step 3: Returns MMS message counts per user
SELECT * FROM mms_messages
