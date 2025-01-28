{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

SELECT 
    created_at::DATE AS date_utc,
    client_type,
    client_version,
    error_code,
    CASE WHEN message_direction = 1 THEN 'incoming'
         WHEN NVL(message_direction, 2) = 2 THEN 'outgoing'
    END AS message_direction,
    IFF(message_direction = 2, COALESCE(message_type, '1'), message_type) AS message_type,
    contact_type,
    country_code,
    city_name,
    COUNT(username) AS num_messages,
    COUNT(DISTINCT username) AS distinct_usernames
FROM {{ source('core', 'messages') }}
WHERE
    (created_at >= {{ var('ds') }}::DATE - INTERVAL '20 DAYS')
    AND (http_response_status <= 299)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY 1
