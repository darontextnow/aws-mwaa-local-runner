{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='DATE_UTC'
    )
}}

WITH use_cases AS (
    SELECT 
        event_id AS update_id, NULLIF(LISTAGG(DISTINCT REGEXP_REPLACE(NULLIF(cases.VALUE::VARCHAR, '9'), 'TN_USE_CASE_'), ',')
          WITHIN GROUP (ORDER BY REGEXP_REPLACE(NULLIF(cases.VALUE::VARCHAR, '9'), 'TN_USE_CASE_')), '')   AS use_cases
    FROM {{ source('party_planner_realtime', 'user_updates') }} b, LATERAL FLATTEN(INPUT => b."payload.tn_use_cases") cases
    WHERE NVL(TRIM(user_id_hex), '') <> ''

    {% if is_incremental() %}
        AND created_at >= (SELECT MAX(created_at)::DATE - INTERVAL '14 days' FROM {{ this }})
        AND created_at <  current_date
    {% endif %}
    GROUP BY 1

),

interests AS (
    SELECT 
        event_id AS update_id,
        NULLIF(LISTAGG(DISTINCT REGEXP_REPLACE(NULLIF(cases.VALUE::VARCHAR, '9'), 'INTEREST_'), ',')
          WITHIN GROUP (ORDER BY REGEXP_REPLACE(NULLIF(cases.VALUE::VARCHAR, '9'), 'INTEREST_')), '') AS interests
    FROM {{ source('party_planner_realtime', 'user_updates') }} b, LATERAL FLATTEN(INPUT => b."payload.interests") cases
    WHERE NVL(TRIM(user_id_hex), '') <> ''

    {% if is_incremental() %}
        AND created_at >= (SELECT MAX(created_at)::DATE - INTERVAL '14 days' FROM {{ this }})
        AND created_at <  current_date
    {% endif %}
    GROUP BY 1
),

base AS (
    SELECT 
        event_id                                                                         AS update_id,
        user_id_hex                                                                      AS user_id_hex,
        created_at                                                                       AS created_at,
        NULLIF(TRIM("client_details.client_data.user_data.first_name"), '')              AS first_name,
        NULLIF(TRIM("client_details.client_data.user_data.last_name"), '')               AS last_name,
        REGEXP_REPLACE(NULLIF("payload.gender", 'GENDER_UNKNOWN'), 'GENDER_')            AS gender,
        REGEXP_REPLACE(NULLIF("payload.age_range", 'AGE_RANGE_UNKNOWN'), 'AGE_RANGE_')   AS age_range,
        NULLIF(TRIM("payload.location.continent_code"), '')                              AS continent_code,
        NULLIF(TRIM("payload.location.country_code"), '')                                AS country_code,
        NULLIF(TRIM("payload.location.state_code"), '')                                  AS state_code,
        NULLIF(TRIM("payload.location.zip_code"), '')                                    AS zip_code,
        NULLIF(TRIM("payload.location.city"), '')                                        AS city,
        NULLIF(TRIM("payload.location.area_code"), '')                                   AS area_code,
        "payload.location.coordinates.latitude"                                          AS latitude,
        "payload.location.coordinates.longitude"                                         AS longitude,
        NULLIF(TRIM("payload.recovery_number"), '')                                      AS recovery_number,
        NULLIF(TRIM("payload.business_name"), '')                                        AS business_name,
        NULLIF(TRIM("payload.other_tn_use_case_text"), '')                               AS other_use_cases,
        NULLIF(TRIM("payload.ethnicity"), '')                                            AS ethnicity,
        NULLIF(TRIM("payload.household_income"), '')                                     AS household_income           
    FROM {{ source('party_planner_realtime', 'user_updates') }}
    WHERE NVL(TRIM(user_id_hex), '') <> ''

    {% if is_incremental() %}
        AND created_at >= (SELECT MAX(created_at)::DATE - INTERVAL '14 days' FROM {{ this }})
        AND created_at <  current_date
    {% endif %}

)

SELECT 
        b.user_id_hex                AS user_id_hex,
        b.created_at::DATE           AS date_utc,
        b.created_at                 AS created_at,
        b.first_name                 AS first_name,
        b.last_name                  AS last_name,
        b.gender                     AS gender,
        b.age_range                  AS age_range,
        UPPER(b.continent_code)      AS continent_code,
        UPPER(b.country_code)        AS country_code,
        UPPER(b.state_code)          AS state_code,
        b.zip_code                   AS zip_code,
        b.city                       AS city,
        b.area_code                  AS area_code,
        b.latitude                   AS latitude,
        b.longitude                  AS longitude,
        b.recovery_number            AS recovery_number,
        b.business_name              AS business_name,
        c.use_cases                  AS use_cases,
        b.other_use_cases            AS other_use_cases,
        i.interests                  AS interests,
        b.ethnicity                  AS ethnicity,
        b.household_income           AS household_income              
FROM base b
LEFT JOIN use_cases c ON b.update_id = c.update_id
LEFT JOIN interests i ON b.update_id = i.update_id