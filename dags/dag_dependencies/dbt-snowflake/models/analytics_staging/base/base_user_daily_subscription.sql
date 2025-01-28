{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH user_daily_mdn AS (
    SELECT 
        b.username,
        a.mdn,
        a.date_utc,
        a.network,
        a.carrier,
        a.item_uuid,
        products.product_id,
        products.product_name
    FROM {{ ref('cost_mdn_daily_mrc_cost') }} a
    JOIN {{ ref('base_user_mdn_history') }} b ON
        (a.mdn = b.mdn)
        AND (CASE WHEN TRY_TO_NUMERIC(b.item_uuid) IS NULL
            THEN (a.item_uuid = b.item_uuid) ELSE (a.iccid = b.item_uuid) END)
        AND (a.date_utc >= b.from_ts::DATE) -- inclusive for starting
        AND (a.date_utc < NVL(b.to_ts, {{ var('current_date') }})::DATE) -- exclusive for ending, so no overlap.
    JOIN (
        SELECT
            a.item_uuid,
            a.product_id,
            b.name AS product_name
        FROM {{ ref('inventory_items') }} a
        JOIN core.products b ON (a.product_id = b.id)
    ) products ON (a.item_uuid = products.item_uuid)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY b.username, a.date_utc
        --added mdn in ORDER BY to make output consistent as there are multiple mdns per username/date_utc
        ORDER BY NVL(b.to_ts, {{ var('current_date') }}) DESC, b.mdn) = 1
)
SELECT
    date_utc,
    a.username,
    mdn,
    a.network,
    carrier,
    item_uuid,
    product_id,
    product_name,
    b.plan_id,
    c.name AS plan_name,
    b.status,
    CASE WHEN c.price = 0 THEN TRUE ELSE FALSE END AS is_free,
    CASE
        WHEN c.data = 1 AND c.price = 0 THEN 'NWTT'
        WHEN c.data = 1 AND c.price > 0 THEN 'Talk & Text'
        WHEN c.data < 1000 THEN c.data || ' MB'
        WHEN c.data < 23552 THEN (c.data / 1024)::INT || ' GB'
        WHEN c.data >= 23552 THEN 'Unlimited'
        ELSE 'others'
    END AS plan_family
FROM user_daily_mdn a
JOIN {{ ref('base_user_subscription_history') }} b ON
    (a.username = b.username)
    AND (a.date_utc >= b.from_ts::DATE)
    AND (a.date_utc < NVL(b.to_ts, {{ var('current_date') }})::DATE)
LEFT JOIN {{ source('base', 'plans') }} c ON (b.plan_id = c.id)
WHERE (status IN ('ACTIVE', 'THROTTLED'))
ORDER BY 1, 2, 3
