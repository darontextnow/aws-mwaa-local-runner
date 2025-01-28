{{
    config(
        tags=['daily_features'],
        full_refresh = false,
        materialized='incremental',
        unique_key='date_utc'
    )
}}

WITH use_case_update AS (
    SELECT
        COALESCE(a.username, u.username) AS updated_username,
        updated_at::DATE AS use_case_update_date,
        updated_at,
        LOWER(SUBSTRING(use_case,13)) AS use_case
    FROM {{ ref('user_profile_use_cases_updates') }} a
    LEFT JOIN {{ ref('analytics_users') }} u USING (user_id_hex)
    WHERE (updated_username IS NOT NULL)
    QUALIFY DENSE_RANK() OVER (PARTITION BY use_case_update_date, updated_username ORDER BY updated_at DESC) = 1
),
use_case_by_date AS (
    SELECT
        updated_username AS username,
        use_case_update_date,
        LISTAGG(DISTINCT use_case, ', ') WITHIN GROUP (ORDER BY use_case) AS use_case
    FROM use_case_update
    GROUP BY 1, 2
)

SELECT DISTINCT
    date_utc,
    a.username,
    use_case_update_date,
    use_case
FROM {{ ref('user_features_active_device') }} a
LEFT JOIN use_case_by_date u ON (a.username = u.username) AND (a.date_utc >= u.use_case_update_date)
WHERE (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '5 DAYS' AND {{ var('ds') }}::DATE)
QUALIFY RANK() OVER (PARTITION BY date_utc, a.username ORDER BY use_case_update_date DESC) = 1
