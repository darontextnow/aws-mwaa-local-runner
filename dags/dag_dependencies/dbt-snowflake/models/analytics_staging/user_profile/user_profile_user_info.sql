{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='username'
    )
}}

-- One row contains the recent info for one user

WITH age_range AS (
    SELECT
        username,
        age_range
    FROM {{ ref('user_profile_last_personal_info') }}
    WHERE age_range IS NOT NULL
    ),
    
    gender AS (
    SELECT
        username,
        gender
    FROM {{ ref('user_profile_last_personal_info') }}
    WHERE gender IS NOT NULL
    ),

    user_use_cases AS (
    SELECT
        username,
        use_cases
    FROM {{ ref('user_profile_last_use_cases') }}
    ),

    user_interests AS (
    SELECT
        username,
        interests
    FROM {{ ref('user_profile_last_interests') }}
    ),

    -- Focus on the info provided by users only
    location AS (
    SELECT
        username,
        continent_code,
        country_code,
        state_code,
        city,
        zip_code
        -- area_code area_code is not provided by users
    FROM {{ ref('user_profile_last_location') }}
    WHERE location_source = 'LOCATION_SOURCE_USER_PROFILE'
    )

SELECT 
    username,
    age_range,
    gender,
    use_cases,
    interests,
    continent_code,
    country_code,
    state_code,
    city,
    zip_code
FROM age_range
FULL OUTER JOIN gender USING(username)
FULL OUTER JOIN user_use_cases USING(username)
FULL OUTER JOIN user_interests USING(username)
FULL OUTER JOIN location USING(username)