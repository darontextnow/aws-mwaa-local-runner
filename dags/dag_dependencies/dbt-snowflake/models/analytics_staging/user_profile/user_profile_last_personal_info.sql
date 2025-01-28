{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

WITH user_age_range AS (
    SELECT
        username,
        updated_at,
        -- format of use case: 'AGE_RANGE_*'
        SUBSTRING(age_range, 11) AS last_age_range,
        RANK() OVER (PARTITION BY username ORDER BY updated_at DESC) AS ranking
     FROM {{ ref('user_profile_personal_info_updates') }}
    WHERE username IS NOT NULL
    AND age_range != 'AGE_RANGE_UNKNOWN' 
    AND COALESCE(len(age_range), 0) != 0
    ),

    user_gender AS (
    SELECT
        username,
        updated_at,
        -- format of use case: 'GENDER_*'
        SUBSTRING(gender, 8) AS last_gender,
        RANK() OVER (PARTITION BY username ORDER BY updated_at DESC) AS ranking
     FROM {{ ref('user_profile_personal_info_updates') }}
    WHERE username IS NOT NULL
    AND gender != 'GENDER_UNKNOWN'
    AND COALESCE(len(gender), 0) != 0
    )

SELECT 
    username,
    updated_at,
    last_age_range AS age_range,
    NULL AS gender
FROM user_age_range
WHERE ranking = 1
UNION ALL
SELECT 
    username,
    updated_at,
    NULL AS age_range,
    last_gender AS gender
FROM user_gender
WHERE ranking = 1
