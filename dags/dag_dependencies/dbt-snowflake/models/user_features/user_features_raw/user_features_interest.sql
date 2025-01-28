{{
    config(
        tags=['daily_features'],
        full_refresh = false,
        materialized='incremental',
        unique_key='date_utc'
    )
}}

WITH interest_update AS (
    SELECT
        COALESCE(a.username, u.username) AS updated_username,
        updated_at::DATE AS interest_update_date,
        updated_at,
        LOWER(SUBSTRING(interest, 10)) AS interest
    FROM {{ ref('user_profile_interests_updates') }} a
    LEFT JOIN {{ ref('analytics_users') }} u USING (user_id_hex)
    WHERE (updated_username IS NOT NULL)
    QUALIFY DENSE_RANK() OVER (PARTITION BY interest_update_date, updated_username ORDER BY updated_at DESC) = 1
),
interest_by_date AS (
    SELECT DISTINCT
        updated_username AS username,
        interest_update_date,
        LISTAGG(DISTINCT interest, ', ') WITHIN GROUP (ORDER BY interest) AS interest
    FROM interest_update
    GROUP BY 1, 2
)

SELECT DISTINCT
    date_utc,
    a.username,
    interest_update_date,
    interest
FROM {{ ref('user_features_active_device') }} a
LEFT JOIN interest_by_date i ON (a.username = i.username) AND (a.date_utc >= i.interest_update_date)
WHERE (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '5 DAYS' AND {{ var('ds') }}::DATE)
QUALIFY RANK() OVER (PARTITION BY date_utc, a.username ORDER BY interest_update_date DESC) = 1
