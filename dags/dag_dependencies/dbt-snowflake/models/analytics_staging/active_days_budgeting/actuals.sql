{{
    config(
        tags=['weekly'],
        materialized='table',
        unique_key='fiscal_day || cohort_utc || w2pu_flag',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

WITH w2pu_usersets AS (
    SELECT
        cohort_utc AS cohort,
        user_set_id,
        pu AS w2pu_flag
    FROM {{ ref('ua_primary_users_by_week') }}
    WHERE 
        (pu = 1)
        AND (week_num = 2)
        AND (user_set_id IS NOT NULL)
),
userset_activedays AS (
    SELECT
        COALESCE(w.w2pu_flag, 0) AS w2pu_flag,
        TO_DATE(u.cohort_utc) AS cohort_utc,
        TO_DATE(d.date_utc) AS fiscal_day,
        SUM(d.dau) AS active_days
    FROM {{ ref('dau_user_set_active_days') }} d
    JOIN {{ ref('user_sets') }} u ON (d.user_set_id = u.user_set_id)
    LEFT JOIN w2pu_usersets w ON (d.user_set_id = w.user_set_id) AND (d.cohort_utc = w.cohort)
    WHERE
        (d.date_utc >= DATE_TRUNC('YEAR', {{ var('ds') }}::DATE))
        AND (u.first_country_code IN ('US', 'CA'))
        AND (d.user_set_id NOT IN (SELECT set_uuid FROM dau.bad_sets))
    GROUP BY 1, 2, 3
    ORDER BY 2, 3, 4
)
SELECT
    YEAR(cohort_utc) AS cohort_year,
    cohort_utc,
    fiscal_day,
    DATE_TRUNC('month', fiscal_day) AS fiscal_month,
    w2pu_flag,
    CASE
        WHEN (DATE_TRUNC('month', cohort_utc) = DATE_TRUNC('month', fiscal_day)) THEN TRUE
        WHEN (DATE_TRUNC('month', cohort_utc) < DATE_TRUNC('month', fiscal_day)) THEN FALSE
    END AS acquired_in_month,
    SUM(active_days) AS actual_active_days
FROM userset_activedays
WHERE (DATE_TRUNC('month', cohort_utc) <= DATE_TRUNC('month', fiscal_day))
GROUP BY ALL
