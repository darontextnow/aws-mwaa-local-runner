{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='USER_ID_HEX'
    )
}}

WITH update_chunk AS (

    SELECT 
        DISTINCT user_id_hex                                                                            AS user_id_hex,
        MAX(created_at) OVER (PARTITION BY USER_ID_HEX)                                                 AS created_at,
        LAST_VALUE(first_name)      IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS first_name,
        LAST_VALUE(last_name)       IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS last_name,
        LAST_VALUE(gender)          IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS gender,
        LAST_VALUE(age_range)       IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS age_range,
        LAST_VALUE(continent_code)  IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS continent_code,
        LAST_VALUE(country_code)    IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS country_code,
        LAST_VALUE(state_code)      IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS state_code,
        LAST_VALUE(zip_code)        IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS zip_code,
        LAST_VALUE(city)            IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS city,
        LAST_VALUE(area_code)       IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS area_code,
        LAST_VALUE(latitude)        IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS latitude,
        LAST_VALUE(longitude)       IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS longitude,
        LAST_VALUE(recovery_number) IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS recovery_number,
        LAST_VALUE(business_name)   IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS business_name,
        LAST_VALUE(use_cases)       IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS use_cases,
        LAST_VALUE(other_use_cases) IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS other_use_cases,
        LAST_VALUE(interests)       IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS interests,
        LAST_VALUE(ethnicity)       IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS ethnicity,
        LAST_VALUE(household_income)       IGNORE NULLS OVER (PARTITION BY user_id_hex ORDER BY created_at)    AS household_income                    
    FROM {{ ref('user_account_profile_updates') }}

    {% if is_incremental() %}
    WHERE created_at >= (SELECT MAX(created_at)::DATE - INTERVAL '14 days' FROM {{ this }})
    {% endif %}

)

select 
       a.user_id_hex                                        AS user_id_hex,
       a.created_at                                         AS created_at,

{% if is_incremental() %}
       COALESCE(b.first_name, a.first_name)                 AS first_name,
       COALESCE(b.last_name, a.last_name)                   AS last_name,
       COALESCE(b.gender, a.gender)                         AS gender,
       COALESCE(b.age_range, a.age_range)                   AS age_range,
       COALESCE(b.continent_code, a.continent_code)         AS continent_code,
       COALESCE(b.country_code, a.country_code)             AS country_code,
       COALESCE(b.state_code, a.state_code)                 AS state_code,
       COALESCE(b.zip_code, a.zip_code)                     AS zip_code,
       COALESCE(b.city, a.city)                             AS city,
       COALESCE(b.area_code, a.area_code)                   AS area_code,
       NULLIF(coalesce(b.latitude, a.latitude), 0)          AS latitude,
       NULLIF(coalesce(b.longitude, a.longitude), 0)        AS longitude,
       COALESCE(b.recovery_number, a.recovery_number)       AS recovery_number,
       COALESCE(b.business_name, a.business_name)           AS business_name,
       COALESCE(b.use_cases, a.use_cases)                   AS use_cases,
       COALESCE(b.other_use_cases, a.other_use_cases)       AS other_use_cases,
       COALESCE(b.interests, a.interests)                   AS interests,
       COALESCE(b.ethnicity, a.ethnicity)                   AS ethnicity,
       COALESCE(b.household_income, a.household_income)     AS household_income       
{% else %}
       a.first_name                                         AS first_name,
       a.last_name                                          AS last_name,
       a.gender                                             AS gender,
       a.age_range                                          AS age_range,
       a.continent_code                                     AS continent_code,
       a.country_code                                       AS country_code,
       a.state_code                                         AS state_code,
       a.zip_code                                           AS zip_code,
       a.city                                               AS city,
       a.area_code                                          AS area_code,
       NULLIF(a.latitude, 0)                                AS latitude,
       NULLIF(a.longitude, 0)                               AS longitude,
       a.recovery_number                                    AS recovery_number,
       a.business_name                                      AS business_name,
       a.use_cases                                          AS use_cases,
       a.other_use_cases                                    AS other_use_cases,
       a.interests                                          AS interests,
       a.ethnicity                                          AS ethnicity,
       a.household_income                                   AS household_income       
{% endif %}

FROM update_chunk a

{% if is_incremental() %}
LEFT JOIN {{ this }} b ON a.user_id_hex = b.user_id_hex
WHERE a.created_at > NVL(b.created_at, '1990-01-01'::timestamp)
{% endif %}