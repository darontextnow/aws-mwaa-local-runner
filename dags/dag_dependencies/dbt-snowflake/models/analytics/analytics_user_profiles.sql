{{
    config(
        tags=['daily'],
        alias="user_profiles",
        materialized='incremental',
        unique_key='user_id_hex'
    )
}}

WITH new_tbl AS (
    SELECT DISTINCT
           user_id_hex,
           MAX(created_at) OVER (PARTITION BY user_id_hex) AS updated_at,
           LAST_VALUE("client_details.client_data.user_data.username") IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS username,
           LAST_VALUE(NULLIF(TRIM("client_details.client_data.user_data.first_name"), '')) IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS first_name,
           LAST_VALUE(NULLIF(TRIM("client_details.client_data.user_data.last_name"), '')) IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS last_name,
           REGEXP_REPLACE(LAST_VALUE(NULLIF("payload.gender", 'GENDER_UNKNOWN')) IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), 'GENDER_') AS gender,
           REGEXP_REPLACE(LAST_VALUE(NULLIF("payload.age_range", 'AGE_RANGE_UNKNOWN')) IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING), 'AGE_RANGE_') AS age_range,
           LAST_VALUE(NULLIF(TRIM("payload.location.continent_code"), '')) IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS continent_code,
           LAST_VALUE(NULLIF(TRIM("payload.location.country_code"), '')) IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS country_code,
           LAST_VALUE(NULLIF(TRIM("payload.location.state_code"), '')) IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS state_code,
           LAST_VALUE(NULLIF(TRIM("payload.location.zip_code"), '')) IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS zip_code,
           LAST_VALUE(NULLIF(TRIM("payload.location.city"), '')) IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS city,
           LAST_VALUE(NULLIF(TRIM("payload.location.area_code"), '')) IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS area_code,
           LAST_VALUE("payload.location.coordinates.latitude") IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS latitude,
           LAST_VALUE("payload.location.coordinates.longitude") IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS longitude,
           LAST_VALUE(NULLIF(TRIM("payload.recovery_number"), '')) IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS recovery_number,
           LAST_VALUE(NULLIF(TRIM("payload.business_name"), '')) IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS business_name,
           LAST_VALUE(NULLIF(TRIM("payload.ethnicity"), '')) IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ethnicity,
           LAST_VALUE(NULLIF(TRIM("payload.household_income"), '')) IGNORE NULLS OVER
               (PARTITION BY user_id_hex ORDER BY created_at
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS household_income
    FROM {{ source('party_planner_realtime', 'user_updates') }}
    {% if is_incremental() %}
    WHERE created_at >= (SELECT MAX(updated_at) - INTERVAL '2 DAYS' FROM {{ this }})
    {% endif %}
)

SELECT new_tbl.user_id_hex,
       new_tbl.updated_at,
       new_tbl.username,
{% if is_incremental() %}
       COALESCE(new_tbl.first_name, cur_tbl.first_name)             AS first_name,
       COALESCE(new_tbl.last_name, cur_tbl.last_name)               AS last_name,
       COALESCE(new_tbl.gender, cur_tbl.gender)                     AS gender,
       COALESCE(new_tbl.age_range, cur_tbl.age_range)               AS age_range,
       COALESCE(new_tbl.continent_code, cur_tbl.continent_code)     AS continent_code,
       COALESCE(new_tbl.country_code, cur_tbl.country_code)         AS country_code,
       COALESCE(new_tbl.state_code, cur_tbl.state_code)             AS state_code,
       COALESCE(new_tbl.zip_code, cur_tbl.zip_code)                 AS zip_code,
       COALESCE(new_tbl.city, cur_tbl.city)                         AS city,
       COALESCE(new_tbl.area_code, cur_tbl.area_code)               AS area_code,
       COALESCE(new_tbl.latitude, cur_tbl.latitude)                 AS latitude,
       COALESCE(new_tbl.longitude, cur_tbl.longitude)               AS longitude,
       COALESCE(new_tbl.recovery_number, cur_tbl.recovery_number)   AS recovery_number,
       COALESCE(new_tbl.business_name, cur_tbl.business_name)       AS business_name,
       COALESCE(new_tbl.ethnicity, cur_tbl.ethnicity)               AS ethnicity,
       COALESCE(new_tbl.household_income, cur_tbl.household_income) AS household_income
{% else %}
       new_tbl.first_name,
       new_tbl.last_name,
       new_tbl.gender,
       new_tbl.age_range,
       new_tbl.continent_code,
       new_tbl.country_code,
       new_tbl.state_code,
       new_tbl.zip_code,
       new_tbl.city,
       new_tbl.area_code,
       new_tbl.latitude,
       new_tbl.longitude,
       new_tbl.recovery_number,
       new_tbl.business_name,
       new_tbl.ethnicity,
       new_tbl.household_income
{% endif %}
    FROM new_tbl
{% if is_incremental() %}

LEFT JOIN {{ this }} AS cur_tbl ON (new_tbl.user_id_hex = cur_tbl.user_id_hex)
WHERE new_tbl.updated_at > cur_tbl.updated_at

{% endif %}
