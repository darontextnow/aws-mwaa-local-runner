{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

SELECT
    date_utc,
    client_type,
    SUM(dau) AS dau,
    SUM(CASE WHEN day_from_cohort = 0 THEN dau ELSE 0 END) AS new_dau,
    SUM(CASE WHEN day_from_cohort = 1 THEN dau ELSE 0 END) AS d1_return_dau,
    SUM(CASE WHEN day_from_cohort = 7 THEN dau ELSE 0 END) AS d7_return_dau,
    SUM(CASE WHEN day_from_cohort = 14 THEN dau ELSE 0 END) AS d14_return_dau,
    SUM(CASE WHEN day_from_cohort = 30 THEN dau ELSE 0 END) AS d30_return_dau
FROM {{ ref('dau_user_set_active_days') }}
WHERE
    (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '1 WEEK' AND {{ var('ds') }}::DATE)
    AND (user_set_id IS NOT NULL)
    AND (user_set_id NOT IN (SELECT set_uuid FROM {{ ref('bad_sets') }}))

GROUP BY 1, 2
