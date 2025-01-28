{{
    config(
        tags=['daily'],
        materialized='view',
        unique_key='date_utc'
    )
}}

SELECT 
    DATE_UTC,
    PLAN_FAMILY,
    SUM(
        CASE
            WHEN IS_FREE = TRUE THEN 1
            ELSE 0
        END
    ) AS Free_Subscription,
    SUM(
        CASE
            WHEN IS_FREE = False THEN 1
            ELSE 0
        END
    ) AS Paid_Subscription,
    SUM(
        CASE
            WHEN plan_id IN (67, 59, 68, 69, 56, 76, 78, 79, 80, 81, 82, 83, 84) THEN 1
            ELSE 0
        END
    ) AS paid_data_plan_users,
    COUNT(1) AS wireless_users
FROM {{ ref('base_user_daily_subscription') }}
WHERE date_utc >= '2021-01-01'
GROUP BY 1, 2
ORDER BY 1 DESC