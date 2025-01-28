{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

-- One row contains a country code for one location source, so one user might have multiple rows if he has multuple location sources

WITH user_location AS (
    SELECT
        username,
        updated_at,
        location_source,
        continent_code,
        country_code,
        state_code,
        city,
        zip_code,
        area_code,
        DENSE_RANK() OVER (PARTITION BY username, location_source ORDER BY updated_at DESC) AS ranking
     FROM {{ ref('user_profile_location_updates') }}
    WHERE username IS NOT NULL
    )

SELECT DISTINCT
    username,
    updated_at,
    location_source,
    continent_code,
    country_code,
    state_code,
    city,
    zip_code,
    area_code
 FROM user_location
WHERE ranking = 1