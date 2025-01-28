{{
    config(
        tags=['weekly'],
        materialized='table',
        unique_key='days_since_acquisition || cohort_year || w2pu_flag',
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
        TO_DATE(d.cohort_utc) AS userset_acquired_date,
        TO_DATE(d.date_utc) AS activity_date,
        TO_DATE(d.date_utc) - TO_DATE(d.cohort_utc) AS days_since_acquisition,
        SUM(d.dau) AS dau
    FROM {{ ref('dau_user_set_active_days') }} d
    JOIN {{ ref('user_sets') }} u ON (d.user_set_id = u.user_set_id)
    LEFT JOIN w2pu_usersets w ON (d.user_set_id = w.user_set_id) AND (d.cohort_utc = w.cohort)
    WHERE
        (d.cohort_utc BETWEEN '2017-01-01' AND DATE_FROM_PARTS(YEAR({{ var('ds') }}::DATE)-1, 12, 31))
        AND (d.date_utc BETWEEN '2017-01-01' AND DATE_FROM_PARTS(YEAR({{ var('ds') }}::DATE)-1, 12, 31))
        AND (u.first_country_code IN ('US', 'CA'))
        AND (d.user_set_id NOT IN (SELECT set_uuid FROM dau.bad_sets))
    GROUP BY 1, 2, 3, 4
    ORDER BY 2, 3, 4
),
dailyretention_actuals AS (
    SELECT
        YEAR(a.userset_acquired_date) AS cohort_year,
        DATE_TRUNC('YEAR', a.userset_acquired_date) AS startofyear,
        a.w2pu_flag,
        a.days_since_acquisition,
        SUM(a.dau) AS numerator,
        SUM(b.dau) AS denominator,
        DIV0(numerator, denominator)::FLOAT AS daily_retention
    FROM userset_activedays a
    JOIN userset_activedays b
        ON (a.userset_acquired_date = b.userset_acquired_date)
        AND (a.w2pu_flag=b.w2pu_flag)
        AND (b.days_since_acquisition = 0)
        AND (a.days_since_acquisition <= DATEDIFF('day', a.userset_acquired_date,
            DATE_FROM_PARTS(YEAR({{ var('ds') }}::DATE)-1, 12, 31)))
    WHERE a.userset_acquired_date <= a.activity_date
    GROUP BY 1, 2, 3, 4
    HAVING denominator > 300 -- Arbitrary threshold to help avoid discontinuity in data
    ORDER BY days_since_acquisition, cohort_year
),
dailyretention_actuals_2017imputebase AS (
    SELECT
        cohort_year,
        w2pu_flag,
        AVG(daily_retention) AS daily_retention_2017imputebase
    FROM dailyretention_actuals
    WHERE
        (cohort_year = 2017)
        AND (days_since_acquisition BETWEEN 2399 AND 2412)
    GROUP BY 1, 2
),
daily_ret_imputed_base AS (
    SELECT
        b.cohort_year,
        b.startofyear,
        w.w2pu_flag,
        b.days_since_acquisition,
        NVL(d.numerator, 0) AS numerator,
        NVL(d.denominator, 0) AS denominator,
        CASE
            WHEN b.cohort_year = 2017 AND d.numerator IS NULL AND d.denominator IS NULL
                THEN i.daily_retention_2017imputebase
            ELSE NVL(d.daily_retention, 0)
        END AS daily_retention,
        CASE
            WHEN d.numerator IS NULL AND d.denominator IS NULL THEN 'Yes'
            ELSE 'No'
        END AS row_needs_imputing
    FROM (
        SELECT DISTINCT
            YEAR(years.startofyear) AS cohort_year,
            years.startofyear,
            DATEDIFF('day', years.startofyear, days.date_utc) AS days_since_acquisition
        FROM (
            SELECT DATE_TRUNC('year',date_utc) AS startofyear
            FROM {{ source('support', 'dates') }}
            WHERE (date_utc BETWEEN '2017-01-01' AND DATE_FROM_PARTS(YEAR({{ var('ds') }}::DATE) - 1, 12, 31))
        ) AS years
        JOIN {{ source('support', 'dates') }} AS days
        WHERE
            (days.date_utc BETWEEN '2017-01-01' AND DATE_FROM_PARTS(YEAR({{ var('ds') }}::DATE) - 1, 12, 31))
            AND (days.date_utc >= years.startofyear)
    ) b
    JOIN (SELECT DISTINCT w2pu_flag FROM dailyretention_actuals) w
    LEFT JOIN dailyretention_actuals d
        ON (d.cohort_year = b.cohort_year)
        AND (d.startofyear = b.startofyear)
        AND (d.w2pu_flag = w.w2pu_flag)
        AND (d.days_since_acquisition = b.days_since_acquisition)
    LEFT JOIN dailyretention_actuals_2017imputebase i
        ON (b.cohort_year = i.cohort_year)
        AND (w.w2pu_flag = i.w2pu_flag)
    UNION ALL
    SELECT DISTINCT
        a.cohort_year,
        a.startofyear,
        a.w2pu_flag,
        DATEDIFF('day', a.startofyear, f.fiscal_day) AS days_since_acquisition,
        0 AS numerator,
        0 AS denominator,
        CASE WHEN a.cohort_year = 2017 THEN i.daily_retention_2017imputebase ELSE 0 END AS daily_retention,
        'Yes' AS row_needs_imputing
    FROM dailyretention_actuals a
    JOIN (SELECT date_utc AS fiscal_day FROM {{ source('support', 'dates') }} WHERE (YEAR(date_utc) = YEAR({{ var('ds') }}::DATE))) f
        ON (f.fiscal_day >= a.startofyear)
    LEFT JOIN dailyretention_actuals_2017imputebase i
        ON (a.cohort_year = i.cohort_year)
        AND (a.w2pu_flag = i.w2pu_flag)
),
daily_ret_imputed_pre AS (
    SELECT DISTINCT
        a.cohort_year,
        a.w2pu_flag,
        a.days_since_acquisition,
        a.row_needs_imputing,
        a.numerator,
        a.denominator,
        a.daily_retention,
        LAG(a.daily_retention, 1) OVER (PARTITION BY a.cohort_year, a.w2pu_flag ORDER BY a.days_since_acquisition)
            AS cy_prev_retention,
        LAG(a.daily_retention, 2) OVER (PARTITION BY a.cohort_year, a.w2pu_flag ORDER BY a.days_since_acquisition)
            AS cy_prev2_retention,
        b.daily_retention AS py_daily_retention,
        c.daily_retention AS py_prev_retention
    FROM daily_ret_imputed_base a
    LEFT JOIN daily_ret_imputed_base b
        ON (a.w2pu_flag = b.w2pu_flag)
        AND (a.days_since_acquisition = b.days_since_acquisition)
        AND (b.cohort_year = a.cohort_year - 1)
    LEFT JOIN daily_ret_imputed_base c
        ON (a.w2pu_flag = c.w2pu_flag)
        AND (a.days_since_acquisition = c.days_since_acquisition - 1)
        AND (c.cohort_year = a.cohort_year - 1)
)
SELECT
    cohort_year,
    w2pu_flag,
    days_since_acquisition,
    row_needs_imputing,
    numerator,
    denominator,
    daily_retention,
    cy_prev_retention,
    cy_prev2_retention,
    py_daily_retention,
    py_prev_retention
FROM daily_ret_imputed_pre
ORDER BY cohort_year, w2pu_flag, days_since_acquisition
