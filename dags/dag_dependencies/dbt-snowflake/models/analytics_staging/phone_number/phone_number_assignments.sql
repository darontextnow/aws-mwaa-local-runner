{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

SELECT
    username,
    area_code || phone_number as phone_number,
    event,
    created_at
FROM {{ source('phone_number_service', 'phone_number_logs') }}
WHERE username != ''