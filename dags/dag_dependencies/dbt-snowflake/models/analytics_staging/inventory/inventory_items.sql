/*
    Sometimes shipped SIMs are reported as BYOSD as a SEPARATE entry
    when they are actived by customers.
    We want to override the activation record with the correct product meta data
*/

{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='item_id',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

WITH src AS (
    SELECT
        id as item_id,
        esn,
        msl,
        mdn,
        msid,
        device_key,
        iccid,
        imsi,
        uuid as item_uuid,
        device_config_id,
        subscription_id as subscription_rate_plan_id,
        product_id,
        state,
        created_at,
        updated_at,
        item_classification,
        qc_count,
        suspended,
        recovery_mode,
        cdma_fallback,
        trued_up,
        throttle_level,
        tethering,
        CASE activation_state
            WHEN 0 THEN 'pending request'
            WHEN 1 THEN 'activated'
            WHEN 2 THEN 'suspended'
            WHEN 3 THEN 'inactive'
        END AS activation_state,
        ip_address
    FROM {{ source('inventory', 'items_data') }}

),

sim AS (
    -- Find ICCID for all shipped SIMs
    SELECT
        iccid,
        MIN(sim_order.item_id) as item_id,
        MIN(products.id) as product_id,
        MIN(sim_order.created_at) as created_at,
        'Shipped' as state
    FROM src sim_order
    JOIN core.products ON sim_order.product_id = products.id
    WHERE
        (products.name LIKE 'SIM (SIM%')
        AND (iccid <> '')
        AND (sim_order.state = 'Shipped')
    GROUP BY iccid

),

sim_activated_as_byosd AS (
    -- All BYOSD entries with matching SIM ICCID
    SELECT DISTINCT iccid
    FROM src
    JOIN sim USING (iccid)
    WHERE (src.product_id = 5)

)

-- rewrite product_id, state and created_at for aforementioned activated BYOSD which are actually SIM
SELECT
    src.item_id,
    sim.item_id as orig_sim_item_id,
    src.esn,
    src.msl,
    src.mdn,
    src.msid,
    src.device_key,
    src.iccid,
    src.imsi,
    src.item_uuid,
    src.device_config_id,
    src.subscription_rate_plan_id,
    sim.product_id,
    sim.state,
    sim.created_at,
    src.updated_at,
    src.item_classification,
    src.qc_count,
    src.suspended,
    src.recovery_mode,
    src.cdma_fallback,
    src.trued_up,
    src.throttle_level,
    src.tethering,
    src.activation_state,
    src.ip_address
FROM src
JOIN sim USING (iccid)
WHERE
    (src.product_id = 5)
    AND (iccid IN (SELECT iccid FROM sim_activated_as_byosd))

UNION ALL SELECT
    src.item_id,
    NULL AS orig_sim_item_id,
    src.esn,
    src.msl,
    src.mdn,
    src.msid,
    src.device_key,
    src.iccid,
    src.imsi,
    src.item_uuid,
    src.device_config_id,
    src.subscription_rate_plan_id,
    src.product_id,
    src.state,
    src.created_at,
    src.updated_at,
    src.item_classification,
    src.qc_count,
    src.suspended,
    src.recovery_mode,
    src.cdma_fallback,
    src.trued_up,
    src.throttle_level,
    src.tethering,
    src.activation_state,
    src.ip_address
FROM src
WHERE (iccid NOT IN (SELECT iccid FROM sim_activated_as_byosd))
 
UNION ALL SELECT
    src.item_id,
    NULL AS orig_sim_item_id,
    src.esn,
    src.msl,
    src.mdn,
    src.msid,
    src.device_key,
    src.iccid,
    src.imsi,
    src.item_uuid,
    src.device_config_id,
    src.subscription_rate_plan_id,
    src.product_id,
    src.state,
    src.created_at,
    src.updated_at,
    src.item_classification,
    src.qc_count,
    src.suspended,
    src.recovery_mode,
    src.cdma_fallback,
    src.trued_up,
    src.throttle_level,
    src.tethering,
    src.activation_state,
    src.ip_address
FROM src
WHERE (iccid IS NULL)
