{{
    config(
        tags=['daily_features'],
        full_refresh = false,
        materialized='incremental',
        unique_key='date_utc'
    )
}}

SELECT
    a.created_at::DATE AS date_utc,
    a.username,
    COUNT(DISTINCT a.id) AS sim_orders
FROM {{ source('inventory', 'orders_data') }} a
JOIN {{ source('inventory', 'order_products') }} b ON (a.id = b.order_id)
WHERE
    (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('ds') }}::DATE)
    AND (a.type = 'NormalOrder')
    AND (b.product_id IN ('392', '428')) -- TMbile SIM
    AND (a.charge_id IS NOT NULL)
    AND (a.id IS NOT NULL)
GROUP BY 1, 2
