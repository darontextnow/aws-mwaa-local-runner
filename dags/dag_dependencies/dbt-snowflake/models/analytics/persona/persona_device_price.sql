{{
    config(
        tags=['weekly'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    ) 
}}

-- * File: persona_device_price.sql
-- * Author: Hao Zhang
-- * Purpose:
--   This query calculates key device-related features for users, including the
--   maximum device price, device age, iOS usage, and Lifeline device status, 
--   aggregated from Android and iOS device data over a 90-day period.
--
-- * Step Overview:
--   - Step 1: Extract distinct Android device details.
--   - Step 2: Extract distinct iOS device details.
--   - Step 3: Map Android device prices and release dates.
--   - Step 4: Map iOS device prices and release dates.
--   - Step 5: Combine Android and iOS data into a unified dataset.
--   - Step 6: Summarize features for each user, including device price, age, and attributes.
--
--  * Vars:
--      @param device_feature_look_back_window: The number of days to look back

WITH android_device_data AS (
-- Step 1: Extract Android device data
    SELECT DISTINCT
        client_platform,
        android_brand_name AS brand_name,
        android_build_id AS build_id,
        android_device_name AS device_name,
        android_hardware_name AS hardware_name,
        android_manufacturer_name AS manufacturer_name,
        android_model_name AS model_name,
        android_product_name AS product_name,
        android_board_name AS board_name,
        android_screen_width AS screen_width,
        android_screen_height AS screen_height,
        user_id_hex
    FROM {{ ref('persona_user_activity_last_90_days') }}
    WHERE
        (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '{{ var('device_feature_look_back_window', 30) }} DAYS'
            AND {{ var('ds') }}::DATE)
        AND (created_date BETWEEN {{ var('ds') }}::DATE - INTERVAL '{{ var('device_feature_look_back_window', 30) }} DAYS'
            AND {{ var('ds') }}::DATE + INTERVAL '1 DAYS')
        AND (client_platform = 'CP_ANDROID')
),

-- Step 2: Extract iOS device data
ios_device_data AS (
    SELECT DISTINCT
        client_platform,
        'Apple' AS brand_name,
        ios_idfv AS idfv,
        ios_model_name AS model_name,
        ios_product_name AS product_name,
        ios_screen_width AS screen_width,
        ios_screen_height AS screen_height,
        user_id_hex
    FROM {{ ref('persona_user_activity_last_90_days') }}
    WHERE
        (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '{{ var('device_feature_look_back_window', 30) }} DAYS'
            AND {{ var('ds') }}::DATE)
        AND (created_date BETWEEN {{ var('ds') }}::DATE - INTERVAL '{{ var('device_feature_look_back_window', 30) }} DAYS'
            AND {{ var('ds') }}::DATE + INTERVAL '1 DAYS')
        AND (client_platform <> 'CP_ANDROID')
),

-- Step 3: Map Android device prices and release dates
android_price_data AS (
    SELECT
        user_id_hex,
        price,
        release_date,
        0 AS is_ios_user,
        (i.brand_name ILIKE ANY ('cloud_mobile', 'vortex', 'foxxd', 'whoop', 'maxwest'))::INT AS is_lifeline_device
    FROM android_device_data AS i
    JOIN {{ source('support', 'device_model_release_date_price') }} AS p ON (i.brand_name = p.brand_name AND i.manufacturer_name = p.manufacturer_name)
    WHERE array_contains(i.model_name::variant, p.model_name)
),

-- Step 4: Map iOS device prices and release dates
ios_price_data AS (
    SELECT
        user_id_hex,
        price,
        release_date,
        1 AS is_ios_user,
        0 AS is_lifeline_device
    FROM ios_device_data AS i
    JOIN {{ source('support', 'ios_device_model_release_date_price') }} AS p ON (i.product_name = p.product_name)
),

-- Step 5: Combine Android and iOS data
combined_device_data AS (
    SELECT * FROM android_price_data
    UNION ALL SELECT * FROM ios_price_data
)

-- Step 6: Summarize device features for each user
SELECT
    user_id_hex,
    MAX(price) AS max_device_price_12mo,
    DATEDIFF(MONTH, MAX(release_date), {{ var('ds') }}::DATE + INTERVAL '1 DAYS') AS newest_device_age_in_months,
    MAX(is_ios_user)::INT AS is_ios_user,
    MAX(is_lifeline_device)::INT AS has_lifeline_device
FROM combined_device_data
GROUP BY 1
