{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}


SELECT
    username,
    cost_mdn_daily_tmobile_usage_cost.*
FROM {{ ref('cost_mdn_daily_tmobile_usage_cost') }}
JOIN {{ ref('inventory_mdn_daily_username') }} USING (mdn, date_utc)
