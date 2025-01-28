{{
    config(
        tags=['weekly'],
        materialized='table',
        unique_key='fiscal_day || cohort_utc || w2pu_flag',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

WITH active_days_budget AS (
    SELECT
        cohort_year,
        cohort_utc,
        fiscal_day,
        fiscal_month,
        w2pu_flag,
        acquired_in_month,
        projected_active_days
    FROM {{ ref('budget_existing_users') }}
    UNION ALL
    SELECT
        cohort_year,
        cohort_utc,
        fiscal_day,
        fiscal_month,
        w2pu_flag,
        acquired_in_month,
        projected_active_days
    FROM {{ ref('budget_new_users') }}
)
SELECT
    b.cohort_year,
    b.cohort_utc,
    b.fiscal_day,
    b.fiscal_month,
    b.w2pu_flag,
    b.acquired_in_month,
    b.projected_active_days AS budget,
    a.actual_active_days AS actual_active_days
FROM active_days_budget b
LEFT JOIN {{ ref('actuals') }} a
    ON (a.cohort_year = b.cohort_year)
    AND (a.cohort_utc = b.cohort_utc)
    AND (a.fiscal_day = b.fiscal_day)
    AND (a.fiscal_month = b.fiscal_month)
    AND (a.w2pu_flag = b.w2pu_flag)
    AND (a.acquired_in_month = b.acquired_in_month)
