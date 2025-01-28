{{
    config(
        tags=['weekly'],
        materialized='table',
        unique_key='days_since_acquisition || cohort_year || w2pu_flag',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH RECURSIVE daily_ret_imputed AS (
    SELECT
        1 AS iteration,
        a.cohort_year,
        a.w2pu_flag,
        a.days_since_acquisition,
        a.row_needs_imputing,
        a.py_daily_retention,
        a.py_prev_retention,
        a.daily_retention
    FROM {{ ref('daily_retention_existing_users_base') }} a
    WHERE row_needs_imputing = 'No' --Base table to start the recursion
    UNION ALL
    SELECT
        rc.iteration + 1,
        t.cohort_year,
        t.w2pu_flag,
        t.days_since_acquisition,
        t.row_needs_imputing,
        t.py_daily_retention,
        t.py_prev_retention,
        CASE
            WHEN rc.cohort_year = 2017 THEN NVL(t.daily_retention, rc.daily_retention)
            WHEN rc.cohort_year >= 2018 THEN rc.daily_retention * DIV0(t.py_prev_retention, t.py_daily_retention)
        END AS daily_retention
    FROM daily_ret_imputed rc
    JOIN {{ ref('daily_retention_existing_users_base') }} t
        ON rc.iteration <= 500 -- recursion iteration stop value
        AND t.cohort_year = rc.cohort_year
        AND t.w2pu_flag = rc.w2pu_flag
        AND rc.days_since_acquisition = t.days_since_acquisition - 1
        AND t.row_needs_imputing = 'Yes'
)
SELECT
    iteration,
    cohort_year,
    w2pu_flag,
    days_since_acquisition,
    row_needs_imputing,
    py_daily_retention,
    py_prev_retention,
    daily_retention
FROM daily_ret_imputed
