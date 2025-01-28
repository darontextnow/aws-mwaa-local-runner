{{
    config(
        tags=['daily_features'],
        full_refresh = false,
        materialized='incremental',
        unique_key='date_utc'
    )
}}

WITH gender_by_date AS (
    SELECT
        username,
        updated_at AS gender_update_at,
        gender
    FROM {{ ref('user_profile_personal_info_updates') }}
    WHERE (gender != 'GENDER_UNKNOWN')
)

SELECT DISTINCT date_utc,
    a.username,
    gender_update_at::DATE AS gender_update_date,
    gender
FROM {{ ref('user_features_active_device') }} a
LEFT JOIN gender_by_date g ON (a.username = g.username) AND (a.date_utc >= g.gender_update_at::DATE)
WHERE (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '5 DAYS' AND {{ var('ds') }}::DATE)
QUALIFY RANK() OVER (PARTITION BY date_utc, a.username ORDER BY gender_update_date DESC) = 1
