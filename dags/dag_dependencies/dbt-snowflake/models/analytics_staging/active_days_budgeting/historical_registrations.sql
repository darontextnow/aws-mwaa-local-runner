{{
    config(
        tags=['weekly'],
        materialized='table',
        unique_key='cohort_utc || w2pu_flag',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH w2pu_usersets AS (
    SELECT
        cohort_utc,
        user_set_id,
        pu AS w2pu_flag
    FROM {{ ref('ua_primary_users_by_week') }}
    WHERE
        (pu = 1)
        AND (week_num = 2)
        AND (user_set_id IS NOT NULL)
)
SELECT
    TO_DATE(a.cohort_utc) AS cohort_utc,
    NVL(w2pu_flag, 0) AS w2pu_flag,
    SUM(dau) AS registrations
FROM {{ ref('dau_user_set_active_days') }} a
LEFT JOIN w2pu_usersets z ON (a.user_set_id = z.user_set_id) AND (a.cohort_utc = z.cohort_utc)
JOIN {{ ref('user_sets') }} b ON (a.user_set_id = b.user_set_id)
WHERE
    (a.user_set_id NOT IN (SELECT set_uuid FROM dau.bad_sets))
    AND (b.first_country_code IN ('US', 'CA'))
    AND (a.date_utc = a.cohort_utc)
    AND (a.date_utc BETWEEN '2017-01-01' AND DATE_FROM_PARTS(YEAR({{ var('ds') }}::DATE)-1, 12, 31))
GROUP BY 1, 2
ORDER BY 1, 2
