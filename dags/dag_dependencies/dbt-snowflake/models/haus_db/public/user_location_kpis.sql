{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='time',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

--note this data is shared externally with external partner HAUS via a separate database
--Their query activity does not show up in our account.

{% set max_date_minus_7_query %}
    SELECT MAX(time) - INTERVAL '7 days' FROM {{ this }}
{% endset %}

{% if execute and flags.WHICH in ["run", "build"] %}
    {% set max_date_minus_7 = run_query(max_date_minus_7_query).columns[0][0] %}
{% else %}
    {% set max_date_minus_7 = modules.datetime.date.today() - modules.datetime.timedelta(days=7) %}
{% endif %}


SELECT
    date_utc as time,
    ip_country_code AS country,
    'ZIP5' AS region_type,
    ip_postal_code AS region,
    (CASE WHEN client_type = 'TN_ANDROID' THEN 'installs_android'
         WHEN client_type = 'TN_IOS_FREE' THEN 'installs_ios' END) AS kpi_name,
    COUNT(*) AS value
FROM {{ ref('haus_installs_staging') }}
WHERE

{% if is_incremental() %}
    (time >= '{{ max_date_minus_7 }}'::DATE)
{% else %}
    (time > '2023-03-15')
{% endif %}

GROUP BY 1, 2, 3, 4, 5

UNION ALL SELECT
    date_utc as time,
    ip_country_code AS country,
    'ZIP5' AS region_type,
    ip_postal_code AS region,
    'installs_total' AS kpi_name,
    COUNT(*) AS value
FROM {{ ref('haus_installs_staging') }}
WHERE

{% if is_incremental() %}
    (time >= '{{ max_date_minus_7 }}'::DATE)
{% else %}
    (time > '2023-03-15')
{% endif %}

GROUP BY 1, 2, 3, 4

UNION ALL SELECT
    date_utc as time,
    ip_country_code AS country,
    'ZIP5' AS region_type,
    ip_postal_code AS region,
    (CASE WHEN client_type = 'TN_ANDROID' THEN 'registrations_android'
         WHEN client_type = 'TN_IOS_FREE' THEN 'registrations_ios' END) AS kpi_name,
    COUNT(*) AS value
FROM {{ ref('haus_registrations_staging') }}
WHERE

{% if is_incremental() %}
    (time >= '{{ max_date_minus_7 }}'::DATE)
{% else %}
    (time > '2023-03-15')
{% endif %}

GROUP BY 1, 2, 3, 4, 5

UNION ALL SELECT
    date_utc as time,
    ip_country_code AS country,
    'ZIP5' AS region_type,
    ip_postal_code AS region,
    'registrations_total' AS kpi_name,
    COUNT(*) AS value
FROM {{ ref('haus_registrations_staging') }}
WHERE

{% if is_incremental() %}
    (time >= '{{ max_date_minus_7 }}'::DATE)
{% else %}
    (time > '2023-03-15')
{% endif %}

GROUP BY 1, 2, 3, 4

UNION ALL SELECT
    date_utc as time,
    ip_country_code AS country,
    'ZIP5' AS region_type,
    ip_postal_code AS region,
    (CASE WHEN client_type = 'TN_ANDROID' THEN 'early_mover_android'
         WHEN client_type = 'TN_IOS_FREE' THEN 'early_mover_ios' END) AS kpi_name,
    COUNT(*) AS value
FROM {{ ref('haus_early_mover_staging') }}
WHERE

{% if is_incremental() %}
    (time >= '{{ max_date_minus_7 }}'::DATE)
{% else %}
    (time >= '2023-10-01')
{% endif %}

GROUP BY 1, 2, 3, 4, 5

UNION ALL SELECT
    date_utc as time,
    ip_country_code AS country,
    'ZIP5' AS region_type,
    ip_postal_code AS region,
    'early_mover_total' AS kpi_name,
    COUNT(*) AS value
FROM {{ ref('haus_early_mover_staging') }}
WHERE

{% if is_incremental() %}
    (time >= '{{ max_date_minus_7 }}'::DATE)
{% else %}
    (time >= '2023-10-01')
{% endif %}

GROUP BY 1, 2, 3, 4
