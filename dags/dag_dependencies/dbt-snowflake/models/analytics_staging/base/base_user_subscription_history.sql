{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

SELECT
    username,
    period_start,
    period_end,
    plan_id,
    status,
    recurring_bill_id,
    billing_item_id,
    last_updated AS from_ts,
    LEAD(last_updated) OVER (PARTITION BY username ORDER BY last_updated) AS to_ts
FROM {{ source('core', 'subscriptions_history') }}
WHERE (username NOT IN ('attilaprod1', 'textnow'))
