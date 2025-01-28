{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}


SELECT
    username,
    product_id,
    mdn,
    uuid AS item_uuid,
    CASE
        WHEN updated_at = MIN(updated_at) OVER (PARTITION BY uuid) THEN created_at - INTERVAL '1 hour'
        ELSE updated_at
    END AS from_ts,
    LEAD(updated_at) OVER (PARTITION BY uuid ORDER BY updated_at) AS to_ts
FROM {{ source('core', 'data_devices_history') }}
