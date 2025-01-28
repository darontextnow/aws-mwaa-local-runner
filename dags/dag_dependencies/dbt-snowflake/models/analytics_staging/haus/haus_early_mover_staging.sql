{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

{% set max_date_minus_3_query %}
    SELECT MAX(date_utc) - INTERVAL '3 days' FROM {{ this }}
{% endset %}

{% if execute and flags.WHICH in ["run", "build"] %}
    {% set max_date_minus_3 = run_query(max_date_minus_3_query).columns[0][0] %}
{% else %}
    {% set max_date_minus_3 = modules.datetime.date.today() - modules.datetime.timedelta(days=3) %}
{% endif %}

WITH latest_geo AS (
    SELECT
        ip_address,
        countrty_iso_code,
        postal_code
    FROM {{ source('core', 'ip_geo_info') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ip_address ORDER BY update_timestamp DESC) = 1
)

SELECT
    em.installed_at::DATE AS date_utc,
    em.adjust_id,
    LPAD(REGEXP_SUBSTR(ip.postal_code, '^[^.]+'), 5, '0') AS ip_postal_code,
    UPPER(ip.countrty_iso_code) AS ip_country_code,
    em.client_type,
    em.ip_address
FROM {{ ref('ua_early_mover_event') }} em
LEFT JOIN latest_geo ip ON em.ip_address = ip.ip_address
WHERE ip_country_code = 'US'
    AND em.client_type IN ('TN_ANDROID', 'TN_IOS_FREE')
    AND ((em.outbound_call = 1) OR (em.lp_sessions = 1)) -- OR condition is neccessary

{% if is_incremental() %}
    AND (em.installed_at::DATE >= '{{ max_date_minus_3 }}'::DATE)
{% else %}
    AND (em.installed_at::DATE > '2023-10-01')
{% endif %}