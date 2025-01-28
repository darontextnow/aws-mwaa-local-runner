/*
    This table keeps a history of mdn / rate plan assigned to every inventory item
*/


{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='surrogate_key',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

WITH item_versions AS (
    SELECT
        -- row_id is used for tie-breaking when event_ts is the same. Some old rows
        -- have have id in reversed order: 4822985 == min(id) where id < 0
        CASE WHEN id < 0 THEN -(id + 4822986) ELSE id END AS row_id,
        item_id,
        -- fill in uuid for older rows; item_id and item_uuid should be bijective
        CASE WHEN uuid <> '' THEN uuid
             ELSE min(nullif(uuid, '')) over (partition by item_id)
        END AS item_uuid,
        COALESCE(mdn, '') AS mdn,
        COALESCE(imsi, '') AS imsi,
        COALESCE(subscription_id, 0) AS subscription_rate_plan_id,
        COALESCE(suspended, False) AS suspended,
        -- event_ts could be null for some old data, fill in using updated_at
        COALESCE(event_ts, updated_at) AS event_ts
    FROM {{ source('inventory' , 'item_versions') }}
),

{{
    log_to_history_cte(
        log_table='item_versions',
        id_column='item_id',
        state_columns=['mdn', 'imsi', 'subscription_rate_plan_id'],
        ts_column='event_ts',
        aux_columns=['item_uuid'],
        order_by=['event_ts', 'row_id']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['item_id', 'from_ts']) }} AS surrogate_key,
    item_id,
    item_uuid,
    mdn,
    imsi,
    subscription_rate_plan_id,
    from_ts,
    to_ts
FROM log_to_history
WHERE
    (mdn <> '')
    AND (subscription_rate_plan_id > 0)  -- keeps only rows with valid internal rate plan
