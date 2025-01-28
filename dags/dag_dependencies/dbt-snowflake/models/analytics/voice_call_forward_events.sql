{{
    config(
        tags=['daily_trust_safety'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

--There is no current usage of this table. However, on 12/20 Diego requested we keep this model active.
SELECT
    date_utc,
    request_ts,
    username,
    client_type,
    client_version,
    client_ip,
    disable_reason,
    route_name,
    http_response_status
FROM {{ source('core', 'tn_requests_raw') }}
WHERE

{% if is_incremental() %}
    (date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 DAY')
{% else %}
    TRUE  --NOTE: source table only retains latest 30 days of history
{% endif %}

    AND (username IS NOT NULL)
    AND (route_name = 'VoiceController_forwardOn')
