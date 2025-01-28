{{
    config(
        tags=['daily'],
        materialized='view',
        unique_key='activation_date'
    )
}}

WITH activations AS (
    SELECT 
        a.username,
        a.date_utc AS activation_date,
        u.created_at AS user_created_at,
        u.user_set_id,
        us.cohort_utc,
        CASE
            WHEN DATEDIFF('day', us.cohort_utc, a.date_utc) <= 7 THEN 'new users'
            ELSE 'existing users'
        END AS userset_segment,
        a.product_id
    FROM 
        {{ ref('base_user_daily_subscription') }} a
        JOIN {{ ref('analytics_users') }} u ON u.username = a.username
        JOIN {{ ref('user_sets') }} us ON us.user_set_id = u.user_set_id
    WHERE a.product_id in (392, 428)
    QUALIFY ROW_NUMBER() OVER ( PARTITION BY a.item_uuid ORDER BY a.date_utc ) = 1
)
SELECT activation_date,
    userset_segment,
    product_id,
    COUNT(user_set_id) AS nbr_user_set_activations
FROM activations
WHERE activation_date >= '2021-01-01'
GROUP BY 1, 2, 3
ORDER BY 1 DESC
