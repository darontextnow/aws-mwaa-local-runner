{{
    config(
        tags=['weekly'],
        materialized='table',
        unique_key='days_since_acquisition || w2pu_flag',
        snowflake_warehouse='PROD_WH_LARGE'
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
        NVL(w.w2pu_flag, 0) AS w2pu_flag,
        COALESCE(d.cohort_utc::DATE, {{ var('ds') }}::DATE) AS cohort_utc,
        d.date_utc::DATE AS activity_date,
        d.date_utc::DATE - d.cohort_utc::DATE AS days_since_acquisition,
        SUM(d.dau) AS dau
    FROM {{ ref('dau_user_set_active_days') }} d
    JOIN {{ ref('user_sets') }} u ON (d.user_set_id = u.user_set_id)
    LEFT JOIN w2pu_usersets w ON (d.user_set_id = w.user_set_id) AND (d.cohort_utc = w.cohort)
    WHERE
        (d.cohort_utc BETWEEN '2022-01-01' AND DATE_FROM_PARTS(YEAR({{ var('ds') }}::DATE), 1, 31))
        AND (d.date_utc BETWEEN '2022-01-01' AND DATE_FROM_PARTS(YEAR({{ var('ds') }}::DATE), 1, 31))
        AND (u.first_country_code IN ('US', 'CA'))
        AND (d.user_set_id NOT IN (SELECT set_uuid FROM dau.bad_sets))
    GROUP BY 1,2,3,4
    ORDER BY 2,3,4
),
daysbase AS (
    SELECT DISTINCT
        days_since_acquisition,
        w2pu_flag
    FROM userset_activedays
    WHERE (days_since_acquisition BETWEEN 0 AND 366)
),
daily_retention_rolling AS (
    SELECT
        b.days_since_acquisition,
        b.w2pu_flag,
        SUM(n.dau) AS numerator,
        SUM(d.dau) AS denominator,
        DIV0(numerator, denominator)::FLOAT AS daily_retention_rolling366d
    FROM daysbase b
    LEFT JOIN userset_activedays n
        ON (b.days_since_acquisition = n.days_since_acquisition)
        AND (b.w2pu_flag = n.w2pu_flag)
        AND (n.cohort_utc BETWEEN DATEADD('day', -(b.days_since_acquisition + 366),
            DATE_FROM_PARTS(YEAR({{ var('ds') }}::DATE), 1, 31))
            AND DATEADD('day', -b.days_since_acquisition, DATE_FROM_PARTS(YEAR({{ var('ds') }}::DATE), 1, 31)))
    LEFT JOIN userset_activedays d
        ON (d.days_since_acquisition = 0)
        AND (d.cohort_utc = n.cohort_utc)
        AND (d.w2pu_flag = n.w2pu_flag)
    GROUP BY 1, 2
)
SELECT
    days_since_acquisition,
    w2pu_flag,
    numerator,
    denominator,
    daily_retention_rolling366d
FROM daily_retention_rolling ORDER BY days_since_acquisition