{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

WITH tz_helper AS (
    SELECT DISTINCT 
        'UTC' || REGEXP_REPLACE(UTC_OFFSET, ':') AS timezone,
        LAST_VALUE(tz_code) IGNORE NULLS OVER (PARTITION BY UTC_OFFSET ORDER BY LAT_LONG) AS tz_code
    FROM {{ source('support', 'timezones') }}
    WHERE (status = 'Canonical')
)

SELECT 
    adjust_id AS ext_device_id,
    client_type AS client_type,
    client_version AS client_version,
    'ADJUST' AS ext_device_id_source,
    'FIRST_INSTALL' AS install_type,
    installed_at AS installed_at,
    IFF(b.tz_code IS NOT NULL, CONVERT_TIMEZONE(b.tz_code, installed_at), NULL) AS local_installed_at,
    click_time AS clicked_at,
    tracker AS tracker,
    store AS store,
    connection_type AS connection_type,
    country_code AS country_code
FROM {{ ref('adjust_installs') }} a
LEFT JOIN tz_helper b ON (a.timezone = b.timezone)

UNION ALL SELECT 
    adjust_id AS ext_device_id,
    client_type AS client_type,
    client_version AS client_version,
    'ADJUST' AS ext_device_id_source,
    'REATTRIBUTION' AS install_type,
    reattributed_at AS installed_at,
    IFF(b.tz_code IS NOT NULL, CONVERT_TIMEZONE(b.tz_code, reattributed_at), NULL) AS local_installed_at,
    click_time AS clicked_at,
    tracker AS tracker,
    store AS store,
    connection_type AS connection_type,
    country_code AS country_code
FROM {{ ref('adjust_reattributions') }} a
LEFT JOIN tz_helper b ON (a.timezone = b.timezone)
