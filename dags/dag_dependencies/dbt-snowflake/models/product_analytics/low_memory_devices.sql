{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

SELECT
    date_utc,
    user_id_hex,
    "client_details.android_bonus_data.product_name" AS product_name,
    {{ pp_normalized_client_type() }} AS client_type,
    COUNT(*) AS total_count
FROM {{ source('party_planner_realtime','property_map') }}
WHERE

{% if is_incremental() %}
    (date_utc > {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
{% else %}
    (date_utc >= '2024-01-01'::DATE)
{% endif %}

    AND (user_id_hex <> '000-00-000-000000000')
    AND ("client_details.client_data.country_code" IN ('MX','CA','US'))
    AND ("payload.properties.EventType" = 'MemoryReport') -- and they are on a low memory device
    AND ("payload.properties.totalMem" <= 2147483648) -- 2147483648 is 2GB
GROUP BY 1, 2, 3, 4
