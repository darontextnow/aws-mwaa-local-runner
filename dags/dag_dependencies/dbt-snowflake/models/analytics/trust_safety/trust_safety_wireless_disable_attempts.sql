{{
    config(
        tags=['daily_trust_safety'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

SELECT
    date_utc,
    request_ts,
    CASE WHEN TRY_TO_NUMERIC(username) IS NOT NULL THEN BINTOHEX(username::INT) ELSE NULL END AS user_id_hex,
    disable_reason
FROM {{ source('core', 'tn_requests_raw') }}
WHERE

{% if is_incremental() %}
    (date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 DAY')
{% else %}
    TRUE  --NOTE this source table only has latest 30 days of data in it.
{% endif %}

    AND (route_name = 'V3DisableUser')
    AND (error_code = 'USER_HAS_SUBSCRIPTION')
