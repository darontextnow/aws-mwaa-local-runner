{{
    config(
        tags=['daily'],
        alias='users',
        materialized='table',
        unique_key='user_id_hex',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}


SELECT
    user_id_hex,
    username,
    set_uuid AS user_set_id,
    created_at
FROM {{ source('core', 'users') }}
LEFT JOIN {{ source('dau', 'user_set') }} USING (username)
