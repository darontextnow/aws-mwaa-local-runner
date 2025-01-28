{{
    config(
        tags=['weekly'],
        materialized='table',
        unique_key='fiscal_day || cohort_utc || w2pu_flag',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH calendar AS (
    SELECT date_utc AS fiscal_day
    FROM {{ source('support', 'dates') }}
    WHERE (YEAR(date_utc) = YEAR({{ var('ds') }}::DATE))
),
-- Compute daily historical registrations - this table is pre-aggregated
hist_regs AS (
    SELECT
        cohort_utc,
        w2pu_flag,
        registrations
    FROM {{ ref('historical_registrations') }}
),
-- Compute daily retention tables - this table is pre-aggregated
daily_ret_actuals AS (
    SELECT
        cohort_year,
        w2pu_flag,
        days_since_acquisition,
        daily_retention
    FROM {{ ref('daily_retention_existing_users') }}
),
-- Produce FY24 budget
budget_existingusers AS (
    SELECT
        h.cohort_utc,
        d.days_since_acquisition,
        f.fiscal_day,
        h.w2pu_flag,
        h.registrations,
        d.daily_retention,
        h.registrations * d.daily_retention AS projected_active_days
    FROM hist_regs h
    JOIN calendar f ON (f.fiscal_day >= h.cohort_utc)
    JOIN daily_ret_actuals d
        ON (d.days_since_acquisition = datediff('day', h.cohort_utc, f.fiscal_day))
        AND (d.cohort_year = year(h.cohort_utc))
        AND (d.w2pu_flag = h.w2pu_flag)
)
SELECT
    YEAR(cohort_utc) AS cohort_year,
    cohort_utc,
    fiscal_day,
    date_trunc('month', fiscal_day) AS fiscal_month,
    w2pu_flag,
    CASE
        WHEN (DATE_TRUNC('month', cohort_utc) = DATE_TRUNC('month', fiscal_day)) THEN TRUE
        WHEN (DATE_TRUNC('month', cohort_utc) < DATE_TRUNC('month', fiscal_day)) THEN FALSE
    END AS acquired_in_month,
    SUM(projected_active_days) AS projected_active_days
FROM budget_existingusers
WHERE (date_trunc('month', cohort_utc) <= date_trunc('month', fiscal_day))
GROUP BY ALL
ORDER BY w2pu_flag, cohort_year, fiscal_day
