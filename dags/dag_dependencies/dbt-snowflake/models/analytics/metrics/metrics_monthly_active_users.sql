{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

-- Generate daily MAUs (the number of users who have been active in the past 30 days) starting from 2018-01-01
-- Web users are excluded and bad user sets are excluded

WITH daily_active_users AS (
    SELECT date_utc AS active_date, client_type, user_set_id
    FROM {{ ref('growth_dau_by_segment') }}
    WHERE
        (date_utc < CURRENT_DATE)
    {% if is_incremental() or target.name == 'dev' %}
        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '36 DAYS')
    {% endif %}
    ORDER BY 1, 2
),
date_range AS (
    SELECT date_utc
    FROM {{ source('support', 'dates') }}
    WHERE
        (date_utc < CURRENT_DATE())
    {% if is_incremental() or target.name == 'dev' %}
        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '7 DAYS')
    {% else %}
        AND (date_utc >= '2018-01-01')
    {% endif %}
    ORDER BY 1
),
rolling_30d_info AS (
    SELECT
        date_utc,    -- the date when user_set_id is considered to be a MAU
        client_type,
        user_set_id
    FROM daily_active_users
    JOIN date_range ON (DATEDIFF(DAY, active_date, date_utc) BETWEEN 0 AND 29) --rolling 30 days
    ORDER BY 1
)

SELECT
    date_utc,
    client_type,
    CASE WHEN first_paid_device_date IS NOT NULL AND first_paid_device_date <= date_utc THEN 'paid'
        ELSE 'organic' END AS channel,
    first_country_code AS country_code,
    COUNT(DISTINCT user_set_id) AS mau
FROM rolling_30d_info
LEFT JOIN {{ ref('user_sets') }} USING (user_set_id)
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
