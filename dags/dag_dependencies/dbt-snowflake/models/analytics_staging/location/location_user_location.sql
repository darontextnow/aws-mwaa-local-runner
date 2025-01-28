{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='username',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

-- If trust GPS data if it's available, otherwise we fall back to Leanplum / IP
WITH user_location AS (
    SELECT * FROM (
        SELECT
            location_source,
            username,
            updated_at,
            country_code,
            state_code,
            city,
            zip_code,
            CASE
                WHEN longitude BETWEEN -180 AND 180 AND latitude BETWEEN -90 AND 90
                THEN ST_MAKEPOINT(longitude, latitude)
            END AS lonlag_coords
        FROM {{ ref('location_user_updates') }}
        WHERE
            (updated_at <= {{ var('ds') }}::DATE)

            {% if is_incremental() %}
                AND (updated_at >= {{ var('ds') }}::DATE - INTERVAL '5 DAYS')
            {% else %}
                AND (updated_at >= '2018-01-01'::DATE)
            {% endif %}

            AND (location_source <> 'AREA_CODE_PHONE_NUMBER_RESERVE_OR_ASSIGN')
-- Note: Commenting out since LP source is depreciated
--        UNION ALL SELECT
--            'LEANPLUM' AS location_source,
--            username,
--            created_at AS updated_at,
--            country_code,
--            "region" AS state_code,
--            city,
--            NULL AS zip_code,
--            ST_MAKEPOINT(lon::FLOAT, lat::FLOAT) lonlag_coords
--        FROM 'location_user_leanplum'
--        WHERE
--            (created_at <= {{ var('ds') }}::DATE)
--
--        {% if is_incremental() %}
--            AND (created_at >= {{ var('ds') }}::DATE - INTERVAL '14 DAYS')
--        {% else %}
--            AND (created_at >= '2018-01-01'::DATE)
--        {% endif %}
    )
    JOIN (
        SELECT 1 AS p, 'COORDINATES_PHONE_NUMBER_RESERVE_OR_ASSIGN' AS location_source
        UNION ALL SELECT 2 AS p, 'USER_PROFILE' AS location_source
        UNION ALL SELECT 3 AS p, 'LEANPLUM' AS location_source
        UNION ALL SELECT 4 AS p, 'IP_PHONE_NUMBER_RESERVE_OR_ASSIGN' AS location_source
        UNION ALL SELECT 5 AS p, 'AREA_CODE_PHONE_NUMBER_RESERVE_OR_ASSIGN' AS location_source
    ) precedence USING (location_source)
),

user_location_ranked_sources AS (
    SELECT
        username,
        MIN(CASE WHEN country_code IS NOT NULL THEN p END) AS country_code_source_p,
        MIN(CASE WHEN state_code IS NOT NULL THEN p END) AS state_code_source_p,
        MIN(CASE WHEN city IS NOT NULL THEN p END) AS city_source_p,
        MIN(CASE WHEN lonlag_coords IS NOT NULL THEN p END) AS lonlag_coords_source_p,
        MIN(CASE WHEN zip_code IS NOT NULL THEN p END) AS zip_code_source_p
    FROM user_location
    GROUP BY username
)

SELECT
    r.username,
    country.country_code,
    country.country_code_source,
    country.country_code_updated_at,
    state.state_code,
    state.state_code_source,
    state.state_code_updated_at,
    city.city,
    city.city_source,
    city.city_updated_at,
    latlong.lonlag_coords,
    latlong.lonlag_coords_source,
    latlong.lonlag_coords_updated_at,
    zip.zip_code,
    zip.zip_code_source,
    zip.zip_code_updated_at
FROM user_location_ranked_sources AS r,
LATERAL (
    SELECT
        country_code,
        location_source AS country_code_source,
        updated_at AS country_code_updated_at
    FROM user_location u
    WHERE (u.username = r.username) AND (u.p = country_code_source_p)
) AS country,
LATERAL (
    SELECT
        state_code,
        location_source AS state_code_source,
        updated_at AS state_code_updated_at
    FROM user_location u
    WHERE (u.username = r.username) AND (u.p = state_code_source_p)
) AS state,
LATERAL (
    SELECT
        city,
        location_source AS city_source,
        updated_at AS city_updated_at
    FROM user_location u
    WHERE (u.username = r.username) AND (u.p = city_source_p)
) AS city,
LATERAL (
    SELECT
        lonlag_coords,
        location_source AS lonlag_coords_source,
        updated_at AS lonlag_coords_updated_at
    FROM user_location u
    WHERE (u.username = r.username) AND (u.p = lonlag_coords_source_p)
)AS latlong,
LATERAL (
    SELECT
        zip_code,
        location_source AS zip_code_source,
        updated_at AS zip_code_updated_at
    FROM user_location u
    WHERE (u.username = r.username) AND (u.p = zip_code_source_p)
) AS zip
