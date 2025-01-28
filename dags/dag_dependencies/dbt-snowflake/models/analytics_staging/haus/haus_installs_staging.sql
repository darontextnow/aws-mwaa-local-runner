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
        geoname_id,
        countrty_iso_code,
        postal_code
    FROM {{ source('core', 'ip_geo_info') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ip_address ORDER BY update_timestamp DESC) = 1
)

SELECT
    DATE(adj.installed_at) AS date_utc,
    adj.adjust_id,
    adj.client_type,
    adj.ip_address,
    adj.postal_code AS adj_postal_code, 
    UPPER(adj.country_code) AS adj_country_code, 
    adj.installed_at,
    LPAD(REGEXP_SUBSTR(ip.postal_code, '^[^.]+'), 5, '0') AS ip_postal_code, 
    UPPER(ip.countrty_iso_code) AS ip_country_code,
    adj.is_organic,
    REGEXP_SUBSTR(ip.geoname_id, '^[^.]+') AS ip_geoname_id,   -- note: using regexp to remove .0 from some values
    UPPER(geo.SUBDIVISION_1_NAME) AS subdivision_1_name,
    UPPER(geo.CITY_NAME) AS city_name
FROM {{ ref('adjust_installs') }} adj
LEFT JOIN latest_geo ip ON adj.ip_address = ip.ip_address
LEFT JOIN {{ source('analytics', 'maxmind_geoip2_city_locations_en') }} geo ON ip_geoname_id = geo.geoname_id
WHERE
    (COALESCE(ip_country_code, adj_country_code) = 'US')
    AND (adj.client_type NOT IN ('2L_ANDROID', 'TN_IOS'))
    AND (adj.is_untrusted = FALSE)

{% if is_incremental() %}
    AND (date_utc >= '{{ max_date_minus_3 }}'::DATE)
{% else %}
    AND (date_utc > '2023-03-15')
{% endif %}