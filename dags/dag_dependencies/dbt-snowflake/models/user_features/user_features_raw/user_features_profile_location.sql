{{
    config(
        tags=['daily_features'],
        full_refresh = false,
        materialized='incremental',
        unique_key='date_utc'
    )
}}

WITH profile_location_by_date AS (
    SELECT
        username,
        updated_at AS profile_location_update_at,
        country_code,
        state_code,
        city,
        zip_code
    FROM {{ ref('user_profile_location_updates') }}
    WHERE (location_source = 'LOCATION_SOURCE_USER_PROFILE')
)

SELECT DISTINCT
    date_utc,
    a.username,
    profile_location_update_at::DATE AS profile_location_update_date,
    country_code,
    state_code,
    city,
    zip_code
FROM {{ ref('user_features_active_device') }} a
LEFT JOIN profile_location_by_date l ON (a.username = l.username) AND (a.date_utc >= l.profile_location_update_at::DATE)
WHERE (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('ds') }}::DATE)
QUALIFY RANK() OVER (PARTITION BY date_utc, a.username ORDER BY profile_location_update_at DESC) = 1
