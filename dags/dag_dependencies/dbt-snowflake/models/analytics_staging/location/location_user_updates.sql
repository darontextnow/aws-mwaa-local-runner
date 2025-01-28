{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='surrogate_key',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

SELECT
    "client_details.client_data.user_data.username" AS username,
    REPLACE("payload.location.location_source", 'LOCATION_SOURCE_') AS location_source,
    username || '-' || location_source AS surrogate_key,
    LAST_VALUE(NULLIF("payload.location.area_code", '')) IGNORE NULLS
        OVER (PARTITION BY username, location_source ORDER BY created_at)::VARCHAR(5) AS area_code,
    LAST_VALUE(NULLIF("payload.location.continent_code", '')) IGNORE NULLS
        OVER (PARTITION BY username, location_source ORDER BY created_at)::VARCHAR(2) AS continent_code,
    LAST_VALUE(NULLIF("payload.location.country_code", '')) IGNORE NULLS
        OVER (PARTITION BY username, location_source ORDER BY created_at)::VARCHAR(2) AS country_code,
    LAST_VALUE(NULLIF("payload.location.state_code", '')) IGNORE NULLS
        OVER (PARTITION BY username, location_source ORDER BY created_at)::VARCHAR(5) AS state_code,
    LAST_VALUE(NULLIF("payload.location.city", '')) IGNORE NULLS
        OVER (PARTITION BY username, location_source ORDER BY created_at)::VARCHAR(50) AS city,
    LAST_VALUE(NULLIF("payload.location.zip_code", '')) IGNORE NULLS
        OVER (PARTITION BY username, location_source ORDER BY created_at)::VARCHAR(10) AS zip_code,
    LAST_VALUE(NULLIF("payload.location.coordinates.latitude", 0)) IGNORE NULLS
        OVER (PARTITION BY username, location_source ORDER BY created_at) AS latitude,
    LAST_VALUE(NULLIF("payload.location.coordinates.longitude", 0)) IGNORE NULLS
        OVER (PARTITION BY username, location_source ORDER BY created_at) AS longitude,
    created_at AS updated_at
FROM {{ source('party_planner_realtime', 'user_updates') }}
WHERE 
    (created_at <= {{ var('ds') }}::DATE)

{% if is_incremental() %}
    AND (created_at >= {{ var('ds') }}::DATE - INTERVAL '5 DAYS')
{% else %}
    AND (created_at >= '2018-01-01'::DATE)
{% endif %}

QUALIFY ROW_NUMBER() OVER (PARTITION BY username, location_source ORDER BY created_at DESC) = 1
