{{
    config(
        tags=['daily_features'],
        full_refresh = false,
        materialized='incremental',
        unique_key='date_utc'
    )
}}

WITH age_range_by_date AS (
    SELECT
        username,
        updated_at AS age_range_update_at,
        age_range
    FROM {{ ref('user_profile_personal_info_updates') }}
    WHERE (age_range != 'AGE_RANGE_UNKNOWN')
)

SELECT DISTINCT
    date_utc,
    a.username,
    age_range_update_at::DATE AS age_range_update_date,
    age_range
FROM {{ ref('user_features_active_device') }} a
LEFT JOIN age_range_by_date age ON (a.username = age.username) AND (a.date_utc >= age.age_range_update_at::DATE)
WHERE (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '5 DAYS' AND {{ var('ds') }}::DATE)
QUALIFY RANK() OVER (PARTITION BY date_utc, a.username ORDER BY age_range_update_at DESC) = 1
