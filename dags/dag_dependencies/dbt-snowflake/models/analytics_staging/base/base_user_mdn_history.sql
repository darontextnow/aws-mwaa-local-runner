{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

WITH log_table AS (
    SELECT
        username,
        uuid AS item_uuid,
        product_id,
        mdn,
        updated_at
    FROM {{ source('core', 'data_devices_history') }}
    WHERE (in_use = TRUE)
),

{{
    log_to_history_cte(
        log_table='log_table',
        id_column='username',
        state_columns=['mdn'],
        ts_column='updated_at',
        aux_columns=['item_uuid', 'product_id']
    )
}}

SELECT * FROM log_to_history
