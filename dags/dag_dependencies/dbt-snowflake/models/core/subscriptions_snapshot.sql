{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE',
        post_hook=[
            "UPDATE {{ this }} tgt
            SET
                device = product_name,
                product_id = order_device.product_id,
                device_presence = 0
            FROM (
                SELECT
                    username,
                    COALESCE(p.name, device_update.new_name) AS product_name,
                    COALESCE(p.id, device_update.product_id) AS product_id,
                    charged_at::DATE AS charged_dt,
                    LEAD(charged_at::DATE) OVER (PARTITION BY username ORDER BY charged_at) AS next_order
                FROM core.wireless_revenue wr
                JOIN {{ source('core', 'users') }} u ON (wr.customer_id = u.stripe_customer_id)
                LEFT JOIN core.products p ON (wr.product_id = p.id)
                LEFT JOIN (
                    SELECT DISTINCT
                        name AS old_name,
                        LAST_VALUE(id) OVER (PARTITION BY NAME ORDER BY updated_at
                             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS product_id,
                        LAST_VALUE(id) OVER (PARTITION BY id ORDER BY updated_at
                             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS new_name
                    FROM core.products_history
                ) device_update ON (wr.product_name = device_update.old_name)
                WHERE
                    (charged_at >= {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
                    AND (classification = 'DeviceAndPlanPurchase')
                    AND (customer_id <> '')
                    AND (stripe_customer_id <> '')
            ) order_device
            WHERE
                (date_utc >= {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
                AND (tgt.device IS NULL)
                AND (tgt.product_id IS NULL)
                AND (tgt.username = order_device.username)
                AND (tgt.date_utc >= order_device.charged_dt)
                AND (tgt.date_utc <  NVL(next_order, SYSDATE()))",

            "UPDATE {{ this }} tgt
            SET
                device = user_device.device,
                product_id = user_device.product_id,
                device_presence = 0
            FROM (
                -- for each user, partition data into continuously active spans
                -- for each partition:
                    --1. fill in rows of null device using the last known device (lag)
                    --2. fill in rows of null device using the next known device (lead) for the first few days
                WITH user_sub_partition AS (
                    SELECT
                        username,
                        date_utc,
                        SUM(gap) OVER (PARTITION BY username ORDER BY date_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                            AS active_span_partition
                    FROM (
                        SELECT
                            username,
                            date_utc,
                            IFF(DATEDIFF(DAY, LAG(date_utc) OVER
                                (PARTITION BY username ORDER BY date_utc), date_utc) > 1, 1, 0)  AS gap
                        FROM {{ this }}
                        WHERE status IN ('ACTIVE', 'THROTTLED', 'DELINQUENT')
                    )
                )
                SELECT
                    username,
                    date_utc,
                    COALESCE(
                    -- in theory when device is not null it trumps everything else,
                    -- but the post_hook update statement above touches only nulls so it doesn't matter here.
                       LAG(device) IGNORE NULLS OVER (PARTITION BY username, active_span_partition ORDER BY date_utc),
                       LEAD(device) IGNORE NULLS OVER (PARTITION BY username, active_span_partition ORDER BY date_utc)
                    ) AS device,
                    COALESCE(
                       LAG(product_id) IGNORE NULLS OVER (PARTITION BY username, active_span_partition ORDER BY date_utc),
                       LEAD(product_id) IGNORE NULLS OVER (PARTITION BY username, active_span_partition ORDER BY date_utc)
                    ) AS product_id
                FROM {{ this }}
                JOIN user_sub_partition USING (date_utc, username)
            ) user_device
            WHERE
                (tgt.username = user_device.username)
                AND (tgt.date_utc = user_device.date_utc)
                AND (tgt.device IS NULL)"
        ]
    )
}}

WITH sub_hist AS (
    SELECT
        id,
        username,
        period_start,
        period_end,
        plan_id,
        status,
        last_updated AS from_ts,
        LEAD(last_updated) OVER (PARTITION BY username ORDER BY last_updated) AS to_ts
    FROM {{ source('core', 'subscriptions_history') }}
    WHERE (username NOT IN ('attilaprod1', 'textnow'))
),
sprint_sim AS (
    SELECT 
        p.name AS product_name,
        sim_order.product_id,
        p.network,
        activation.uuid,
        activation.esn,
        sim_order.uuid AS orig_uuid,
        activation.mdn,
        sim_order.created_at AS shipped_at
    FROM {{ source('inventory', 'items_data') }} sim_order
    JOIN {{ source('inventory', 'items_data') }} activation USING (iccid)
    JOIN core.products p ON sim_order.product_id = p.id
    WHERE 
        (activation.product_id = 5)
        AND (sim_order.state = 'Shipped')
        AND (p.name LIKE 'SIM (SIM%')
),
device_hist AS (
    SELECT 
        dd.username,
        COALESCE(sprint_uuid.product_name, sprint_esn.product_name, p.name) AS product_name,
        COALESCE(sprint_uuid.product_id, sprint_esn.product_id, p.id) AS product_id,
        COALESCE(sprint_uuid.network, sprint_esn.network, p.network) AS network,
        COALESCE(dd.mdn, sprint_uuid.mdn, sprint_esn.mdn) AS mdn,
        COALESCE(sprint_uuid.orig_uuid, sprint_esn.orig_uuid, dd.uuid) AS uuid,
        subscription_id,
        IFF(dd.updated_at = MIN(dd.updated_at) OVER (PARTITION BY dd.username), dd.created_at, dd.updated_at) AS from_ts,
        LEAD(dd.updated_at) OVER (PARTITION BY dd.username ORDER BY dd.updated_at) AS to_ts
    FROM {{ source('core', 'data_devices_history') }} dd
    LEFT JOIN core.products p ON (dd.product_id = p.id)
    LEFT JOIN sprint_sim sprint_uuid ON 
        (dd.uuid = sprint_uuid.uuid)
        AND (dd.product_id = 5)
        AND (sprint_uuid.shipped_at <= dd.updated_at)
    LEFT JOIN sprint_sim sprint_esn ON 
        (dd.esn = sprint_esn.esn)
        AND (dd.product_id = 5)
        AND (sprint_esn.shipped_at <= dd.updated_at)
),
base AS (
    SELECT 
        (date_utc - INTERVAL '1 DAY')::DATE AS date_utc,
        sh.id AS subscription_id,
        sh.username,
        status,
        period_start,
        period_end,
        plan_id,
        product_id,
        product_name AS device,
        network,
        uuid,
        mdn
    FROM {{ source('support', 'dates') }}
    JOIN sub_hist sh ON (date_utc >= sh.from_ts) AND (date_utc < COALESCE(sh.to_ts, sh.period_end, SYSDATE()))
    LEFT JOIN device_hist dh ON
        (date_utc >= dh.from_ts)
        AND (date_utc < COALESCE(dh.to_ts, SYSDATE()))
        AND (sh.username = dh.username)
        AND (sh.id = dh.subscription_id)
    WHERE 
        (date_utc >= {{ var('ds') }}::DATE - INTERVAL '4 DAYS')
        AND (date_utc <= {{ var('data_interval_end') }}::DATE)
),
outgoing_calls AS (
    SELECT 
        date_utc,
        username,
        COUNT(IFF(is_domestic AND is_free, 1, NULL)) AS out_dom_free_calls,
        COUNT(IFF(is_domestic AND NOT is_free, 1, NULL)) AS out_dom_paid_calls,
        COUNT(IFF(NOT is_domestic AND is_free, 1, NULL)) AS out_intl_free_calls,
        COUNT(IFF(NOT is_domestic AND NOT is_free, 1, NULL)) AS out_intl_paid_calls,
        SUM(IFF(is_domestic AND is_free, CALL_SECS, 0)) / 60.0 AS out_dom_free_minutes,
        SUM(IFF(is_domestic AND NOT is_free, CALL_SECS, 0)) / 60.0 AS out_dom_paid_minutes,
        SUM(IFF(NOT is_domestic AND is_free, CALL_SECS, 0)) / 60.0 AS out_intl_free_minutes,
        SUM(IFF(NOT is_domestic AND NOT is_free, CALL_SECS, 0)) / 60.0 AS out_intl_paid_minutes,
        SUM(IFF(is_domestic AND is_free, COST, 0)) AS out_dom_free_cost,
        SUM(IFF(is_domestic AND NOT is_free, COST, 0)) AS out_dom_paid_cost,
        SUM(IFF(NOT is_domestic AND is_free, COST, 0)) AS out_intl_free_cost,
        SUM(IFF(NOT is_domestic AND NOT is_free, COST, 0)) AS out_intl_paid_cost,
        out_dom_free_cost + out_dom_paid_cost + out_intl_free_cost + out_intl_paid_cost AS out_total_cost
    FROM {{ ref ('outgoing_calls') }}
    WHERE
        (date_utc >= {{ var('ds') }}::DATE - INTERVAL '4 DAYS')
        AND (username IN (SELECT username FROM base WHERE status IN ('ACTIVE', 'THROTTLED', 'DELINQUENT')))
    GROUP BY 1, 2
),
incoming_calls AS (
    SELECT 
        date_utc,
        username,
        COUNT(IFF(is_domestic AND is_free, 1, NULL))         AS in_dom_free_calls,
        COUNT(IFF(is_domestic AND NOT is_free, 1, NULL))     AS in_dom_paid_calls,
        COUNT(IFF(NOT is_domestic AND is_free, 1, NULL))     AS in_intl_free_calls,
        COUNT(IFF(NOT is_domestic AND NOT is_free, 1, NULL)) AS in_intl_paid_calls,
        SUM(IFF(is_domestic AND is_free, CALL_SECS, 0)) / 60.0 AS in_dom_free_minutes,
        SUM(IFF(is_domestic AND NOT is_free, CALL_SECS, 0)) / 60.0 AS in_dom_paid_minutes,
        SUM(IFF(NOT is_domestic AND is_free, CALL_SECS, 0)) / 60.0 AS in_intl_free_minutes,
        SUM(IFF(NOT is_domestic AND NOT is_free, CALL_SECS, 0)) / 60.0 AS in_intl_paid_minutes,
        SUM(IFF(is_domestic AND is_free, COST, 0)) AS in_dom_free_cost,
        SUM(IFF(is_domestic AND NOT is_free, COST, 0)) AS in_dom_paid_cost,
        SUM(IFF(NOT is_domestic AND is_free, COST, 0)) AS in_intl_free_cost,
        SUM(IFF(NOT is_domestic AND NOT is_free, COST, 0)) AS in_intl_paid_cost,
        in_dom_free_cost + in_dom_paid_cost + in_intl_free_cost + in_intl_paid_cost AS in_total_cost
    FROM {{ ref ('incoming_calls')}}
    WHERE 
        (date_utc >= {{ var('ds') }}::DATE - INTERVAL '4 DAYS')
        AND (username IN (SELECT username FROM base WHERE status IN ('ACTIVE', 'THROTTLED', 'DELINQUENT')))
    GROUP BY 1, 2
),
messages AS (
    SELECT 
        created_at::DATE AS date_utc,
        username,
        SUM(IFF(NVL(message_direction, 2) = 1, 1, 0)) AS incoming_messages,
        SUM(IFF(NVL(message_direction, 2) = 2, 1, 0)) AS outgoing_messages
    FROM {{ source('core', 'messages') }}
    WHERE 
        (created_at >= {{ var('ds') }}::DATE - INTERVAL '4 DAYS')
        AND (username IN (SELECT username FROM base WHERE status IN ('ACTIVE', 'THROTTLED', 'DELINQUENT')))
    GROUP BY 1, 2
)
SELECT 
    b.date_utc,
    b.subscription_id,
    b.username,
    b.status,
    b.period_start,
    b.period_end,
    p.name AS plan,
    b.plan_id,
    DECODE(b.network, 0, 'Sprint', 1, 'Kore', 2, 'KoreAtt') AS network,
    b.product_id,
    b.device,
    IFF(b.device IS NOT NULL, 1, 0) AS device_presence,
    b.uuid,
    b.mdn,
    NVL(out_dom_free_calls, 0) AS outgoing_dom_free_calls,
    NVL(out_dom_paid_calls, 0) AS outgoing_dom_paid_calls,
    NVL(out_intl_free_calls, 0) AS outgoing_intl_free_calls,
    NVL(out_intl_paid_calls, 0) AS outgoing_intl_paid_calls,
    NVL(in_dom_free_calls, 0) AS incoming_dom_free_calls,
    NVL(in_dom_paid_calls, 0) AS incoming_dom_paid_calls,
    NVL(in_intl_free_calls, 0) AS incoming_intl_free_calls,
    NVL(in_intl_paid_calls, 0) AS incoming_intl_paid_calls,
    NVL(out_dom_free_minutes, 0) AS outgoing_dom_free_minutes,
    NVL(out_dom_paid_minutes, 0) AS outgoing_dom_paid_minutes,
    NVL(out_intl_free_minutes, 0) AS outgoing_intl_free_minutes,
    NVL(out_intl_paid_minutes, 0) AS outgoing_intl_paid_minutes,
    NVL(in_dom_free_minutes, 0) AS incoming_dom_free_minutes,
    NVL(in_dom_paid_minutes, 0) AS incoming_dom_paid_minutes,
    NVL(in_intl_free_minutes, 0) AS incoming_intl_free_minutes,
    NVL(in_intl_paid_minutes, 0) AS incoming_intl_paid_minutes,
    NVL(out_dom_free_cost, 0) + NVL(in_dom_free_cost, 0) AS dom_free_cost,
    NVL(out_dom_paid_cost, 0) + NVL(in_dom_paid_cost, 0) AS dom_paid_cost,
    NVL(out_intl_free_minutes, 0) + NVL(in_intl_free_minutes, 0) AS intl_free_cost,
    NVL(out_intl_paid_minutes, 0) + NVL(in_intl_paid_minutes, 0) AS intl_paid_cost,
    NVL(out_total_cost, 0) + NVL(in_total_cost, 0) AS onvoy_cost,
    NVL(incoming_messages, 0) AS incoming_messages,
    NVL(outgoing_messages, 0) AS outgoing_messages,
    --the following values had been sourced from core.sandvine_data_usage table which has not been populated since 2023
    0 AS total_bytes, -- value has been 0 since 2022-11-04
    0 AS data_cost  -- value has been 0 since 2022-11-04
FROM base b
JOIN {{ source('core', 'plans') }} p ON (b.plan_id = p.id)
LEFT JOIN incoming_calls ic ON (b.date_utc = ic.date_utc) AND (b.username = ic.username)
LEFT JOIN outgoing_calls oc ON (b.date_utc = oc.date_utc) AND (b.username = oc.username)
LEFT JOIN messages m ON (b.date_utc = m.date_utc) AND (b.username = m.username)
WHERE 
    (b.date_utc >= {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
    AND ((status IN ('ACTIVE', 'THROTTLED', 'DELINQUENT')) OR (total_bytes > 0))
ORDER BY 1
