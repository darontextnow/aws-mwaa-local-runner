{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

SELECT
    created_at::DATE AS date_utc,
    username,
    item_category,
    SUM(NVL(paid_amount, 0)) AS revenue
FROM {{ ref('billing_service_purchases') }}
GROUP BY 1, 2, 3
