{{
    config(
        tags=['weekly'],
        materialized='incremental',
        unique_key=['event_id'],
        incremental_strategy='merge',
        post_hook="DELETE FROM {{ this }} WHERE created_date < {{ var('ds') }}::DATE - INTERVAL '91 DAYS';",
        snowflake_warehouse='PROD_WH_MEDIUM'
    ) 
}}

-- * File: persona_user_last_90_days.sql
-- * Author: Hao Zhang
-- * Purpose: 
--   Creates an incremental table to log user activity data, including 
--   session details, device attributes, and geolocation, with local time 
--   conversion.
--

-- * Step Overview:
-- Step 1: Extract raw application lifecycle data.
-- Step 2: Enrich data with geolocation and timezone details.
-- Step 3: Adjust timestamps to local timezones.
-- Step 4: Combine enriched data for output.

--  look_back_window has been set to 90 days (fixed)

WITH data_source AS (
-- Step 1: Extract raw application lifecycle data
-- On first run, load data for the last 90 days prior to the {'ds'}
-- On subsequent runs, load only data for the last 7 days prior to the {'ds'}

    SELECT
        "client_details.client_data.user_data.username" AS username,
        created_at,
        created_date,
        user_id_hex,
        event_id,
        date_utc,
        "client_details.client_data.client_ip_address" AS ip_address,
        "payload.time_in_session" AS time_in_session,
        "client_details.client_data.country_code" AS country_code,
        "client_details.client_data.client_platform" AS client_platform,
        "client_details.android_bonus_data.brand_name" AS android_brand_name,
        "client_details.android_bonus_data.build_id" AS android_build_id,
        "client_details.android_bonus_data.device_name" AS android_device_name,
        "client_details.android_bonus_data.hardware_name" AS android_hardware_name,
        "client_details.android_bonus_data.manufacturer_name" AS android_manufacturer_name,
        "client_details.android_bonus_data.model_name" AS android_model_name,
        "client_details.android_bonus_data.product_name" AS android_product_name,
        "client_details.android_bonus_data.board_name" AS android_board_name,
        "client_details.android_bonus_data.screen_width" AS android_screen_width,
        "client_details.android_bonus_data.radio_height" AS android_screen_height,
        "client_details.ios_bonus_data.idfv" AS ios_idfv,
        "client_details.ios_bonus_data.model_name" AS  ios_model_name,
        "client_details.ios_bonus_data.product_name" AS ios_product_name,
        "client_details.ios_bonus_data.screen_width" AS ios_screen_width,
        "client_details.ios_bonus_data.screen_height" AS ios_screen_height,
    FROM {{ source('party_planner_realtime', 'app_lifecycle_changed') }}
    WHERE (
        {% if is_incremental() %}
            created_date BETWEEN {{ var('ds') }}::DATE - INTERVAL '7 DAYS' 
                AND {{ var('ds') }}::DATE + INTERVAL '1 DAYS'
        {% else %}
            created_date BETWEEN {{ var('ds') }}::DATE - INTERVAL '90 DAYS' 
                AND {{ var('ds') }}::DATE + INTERVAL '1 DAYS'
        {% endif %}
    )
),

-- Step 2: Enrich data with geolocation and timezone information
ip_geo_tzone_info AS (
    SELECT
        ip_address,
        postal_code AS zipcode,
        latitude,
        longitude,
        tzid
    FROM {{ source('core', 'ip_geo_info_latest') }} AS a
    JOIN {{ ref('location_timezone_boundary_h3') }} AS zcta5 ON
        (ST_WITHIN(ST_POINT(a.longitude, a.latitude), zcta5.geometry)
            AND H3_LATLNG_TO_CELL(a.latitude, a.longitude, 2) = zcta5.h3_res2_cell)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.ip_address ORDER BY tzid) = 1
),

-- Step 3: Add local timezone conversion
add_local_time AS (
    SELECT 
        a.*,
        b.zipcode,
        b.latitude,
        b.longitude,
        b.tzid,
        CONVERT_TIMEZONE(b.tzid, a.created_at) AS created_at_local
    FROM data_source AS a
    LEFT JOIN ip_geo_tzone_info AS b ON (a.ip_address = b.ip_address)
)

-- Step 4: Combined enriched data
SELECT * FROM add_local_time
