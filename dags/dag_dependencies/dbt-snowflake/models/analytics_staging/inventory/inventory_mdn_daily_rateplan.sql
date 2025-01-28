{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

WITH hist AS (
    SELECT
        mdn,
        imsi,
        item_uuid,
        subscription_rate_plan_id,
        from_ts,
        to_ts
    FROM {{ ref('inventory_item_rateplan_history') }}
),

daily AS (
    SELECT
        date_utc,
        hist.mdn,
        hist.imsi,
        hist.item_uuid,
        hist.subscription_rate_plan_id,
        ROW_NUMBER() OVER (
            PARTITION BY mdn, date_utc
            ORDER BY from_ts DESC
        ) AS rank  -- we want to keep only the last row when there are duplicates
    FROM hist
    {{ history_to_daily_join('hist.from_ts', 'hist.to_ts') }}
    WHERE (date_utc < CURRENT_DATE)

)

SELECT
    mdn,
    date_utc,
    imsi,
    item_uuid,
    subscription_rate_plan_id
FROM daily
WHERE (rank = 1)
