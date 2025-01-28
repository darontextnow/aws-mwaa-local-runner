{{
    config(
        tags=['weekly'],
        materialized='table',
        unique_key='fiscal_day || cohort_utc || w2pu_flag',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

-- Create shell table to contain budget
WITH calendar AS (
    SELECT date_utc AS fiscal_day
    FROM {{ source('support', 'dates') }}
    WHERE (YEAR(date_utc) = YEAR({{ var('ds') }}::DATE))
),
-- Pull FY24 UA inputs; this is hard-coded data and not necessarily updated throughout the year
-- The goal here is to determine, approximately, how many registrations to expect each day
fy24_ua_inputs AS (
    SELECT
        fiscal_day,
        ua_spend,
        cpi,
        paid_install_to_reg,
        organic_to_paid_reg,
        reg_to_pu
    FROM {{ ref('projected_ua_inputs') }}
),
fy24_regs AS (
    SELECT
        fiscal_day,
        fiscal_day AS cohort_utc,
        0 AS w2pu_flag,
        NVL((ua_spend / cpi * paid_install_to_reg) * (1 + organic_to_paid_reg) * (1 - reg_to_pu), 0)
            AS projected_registrations
    FROM fy24_ua_inputs a
    UNION ALL
    SELECT
        fiscal_day,
        fiscal_day AS cohort_utc,
        1 AS w2pu_flag,
        NVL((ua_spend / cpi * paid_install_to_reg) * (1 + organic_to_paid_reg) * reg_to_pu, 0)
            AS projected_registrations
    FROM fy24_ua_inputs a
),
-- Compute daily retention tables - this table contains numerator & denominator values to use for rolling 366d averages
-- The goal is to create a rolling
daily_ret_actuals AS (
    SELECT
        w2pu_flag,
        days_since_acquisition,
        daily_retention_rolling366d AS daily_retention
    FROM {{ ref('daily_retention_new_users') }}
),
-- Produce FY24 budget for New users
budget_newusers AS (
    SELECT
        n.cohort_utc,
        d.days_since_acquisition,
        f.fiscal_day,
        n.w2pu_flag,
        n.projected_registrations AS registrations,
        d.daily_retention,
        n.projected_registrations * d.daily_retention AS projected_actuals_active_days
    FROM fy24_regs n
    JOIN calendar f ON (f.fiscal_day >= n.cohort_utc)
    JOIN daily_ret_actuals d
        ON (d.days_since_acquisition = datediff('day', n.cohort_utc, f.fiscal_day))
        AND (d.w2pu_flag = n.w2pu_flag)
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
    SUM(projected_actuals_active_days) AS projected_active_days
FROM budget_newusers
WHERE (date_trunc('month', cohort_utc) <= date_trunc('month', fiscal_day))
GROUP BY ALL
ORDER BY w2pu_flag, cohort_year, fiscal_day