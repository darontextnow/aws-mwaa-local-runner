{{
    config(
        tags=['daily'],
        materialized='view',
        unique_key='date_utc'
    )
}}

SELECT 
    a.created_at::DATE AS date_utc,
    CASE
        WHEN UPPER(a.user_agent) ilike '%ANDROID%'
        OR UPPER(a.user_agent) ilike '%IOS%' THEN 'app'
        ELSE 'web'
    END AS purchase_platform,
    CASE
        WHEN DATEDIFF('day', e.cohort_utc, a.created_at) <= 7 THEN 'new users'
        ELSE 'existing users'
    END AS userset_segment,
    CASE
        WHEN charge_id IS NULL THEN 'Free'
        ELSE 'Paid'
    END AS charge_type,
    COUNT(*) AS sim_orders,
    COUNT(DISTINCT u.username) AS sim_nums,
    COUNT(DISTINCT u.user_set_id) AS sim_orders_nbr_usersets
FROM 
    {{ source('inventory', 'orders_data') }} a
    JOIN {{ source('inventory', 'order_products') }} b ON a.id = b.order_id
    JOIN {{ ref('analytics_users') }} u ON a.username = u.username
    JOIN {{ ref('user_sets') }} e ON e.user_set_id = u.user_set_id
    JOIN {{ source('core', 'users') }} f ON f.username = a.username
WHERE
    UPPER(a.type) = 'NORMALORDER'
    AND b.product_id in (392, 428)
    AND a.created_at::DATE >= '2021-01-01'
    AND UPPER(account_status) = 'ENABLED'
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC
