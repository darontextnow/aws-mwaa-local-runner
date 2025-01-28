{{
    config(
        tags=['weekly'],
        materialized='table',
        snowflake_warehouse='PROD_WH_SMALL'
    ) 
}}

-- * File: personas_self_reported.sql
-- * Author: Hao Zhang
-- * Author: Hao Zhang
-- * Purpose:
--   This model creates user-level features based on self-reported demographic
--   data and parsed interests/use cases. It includes use-case flags, 
--   interest indicators, and final feature adjustments for active use cases.

-- * Step Overview:
--   1. Stage user profiles and demographic data.
--   2. Parse and transform use cases and interests into individual flags.
--   3. Adjust use-case features with conditional NULLs for inactive users.

WITH self_reported AS (
-- Step 1: Stage user profiles and demographic data
    SELECT
        a.username,
        a.user_id_hex,
        b.age_range,
        b.use_cases,
        b.interests,
        b.household_income,
        c.best_gender AS gender,
        d.best_ethnicity AS ethnicity
    FROM {{ ref('persona_universe') }} a
    LEFT JOIN {{ ref('user_account_profile') }} AS b ON (a.user_id_hex = b.user_id_hex)
    LEFT JOIN {{ source('analytics', 'demo_gender_pred') }} AS c ON (a.username = c.username)
    LEFT JOIN {{ source('analytics', 'demo_ethnicity_pred') }} AS d ON (a.username = d.username)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

-- Step 2: Parse and transform use cases and interests into individual flags
self_reported_massage AS (
    SELECT
        username,
        user_id_hex,
        age_range,
        household_income,
        gender,
        ethnicity,
        MAX(CASE WHEN POSITION('BACKUP' IN use_cases) > 0 THEN 1 ELSE 0 END) AS use_case_backup,
        MAX(CASE WHEN POSITION('BUSINESS' IN use_cases) > 0 THEN 1 ELSE 0 END) AS use_case_business,
        MAX(CASE WHEN POSITION('BUYING_OR_SELLING' IN use_cases) > 0 THEN 1 ELSE 0 END) AS use_case_buying_or_selling,
        MAX(CASE WHEN POSITION('DATING' IN use_cases) > 0 THEN 1 ELSE 0 END) AS use_case_dating,
        MAX(CASE WHEN POSITION('FOR_WORK' IN use_cases) > 0 THEN 1 ELSE 0 END) AS use_case_for_work,
        MAX(CASE WHEN POSITION('JOB_HUNTING' IN use_cases) > 0 THEN 1 ELSE 0 END) AS use_case_job_hunting,
        MAX(CASE WHEN POSITION('LONG_DISTANCE_CALLING' IN use_cases) > 0 THEN 1 ELSE 0 END) AS use_case_long_distance_calling,
        MAX(CASE WHEN POSITION('OTHER' IN use_cases) > 0 THEN 1 ELSE 0 END) AS use_case_other,
        MAX(CASE WHEN POSITION('PRIMARY' IN use_cases) > 0 THEN 1 ELSE 0 END) AS use_case_primary,
        MAX(CASE WHEN POSITION('AUTOMOTIVE' IN interests) > 0 THEN 1 ELSE 0 END) AS interests_automotive,
        MAX(CASE WHEN POSITION('DATING' IN interests) > 0 THEN 1 ELSE 0 END) AS interests_dating,
        MAX(CASE WHEN POSITION('ENTERTAINMENT' IN interests) > 0 THEN 1 ELSE 0 END) AS interests_entertainment,
        MAX(CASE WHEN POSITION('FINANCE' IN interests) > 0 THEN 1 ELSE 0 END) AS interests_finance,
        MAX(CASE WHEN POSITION('FOOD' IN interests) > 0 THEN 1 ELSE 0 END) AS interests_food,
        MAX(CASE WHEN POSITION('HEALTH_AND_FITNESS' IN interests) > 0 THEN 1 ELSE 0 END) AS interests_health_and_fitness,
        MAX(CASE WHEN POSITION('NEWS' IN interests) > 0 THEN 1 ELSE 0 END) AS interests_news,
        MAX(CASE WHEN POSITION('RETAIL' IN interests) > 0 THEN 1 ELSE 0 END) AS interests_retail,
        MAX(CASE WHEN POSITION('TRAVEL' IN interests) > 0 THEN 1 ELSE 0 END) AS interests_travel,
        MAX(CASE WHEN
            REGEXP_LIKE(use_cases, '.*?(BACKUP|BUSINESS|BUYING_OR_SELLING|DATING|FOR_WORK|JOB_HUNTING|LONG_DISTANCE_CALLING|OTHER|PRIMARY).*?')
            THEN 1 ELSE 0 END
        ) AS any_use_case_active
    FROM self_reported
    GROUP BY 1, 2, 3, 4, 5, 6
)

-- Step 3: Adjust use-case features with conditional NULLs for inactive users
SELECT
    username,
    user_id_hex,
    age_range,
    household_income,
    gender,
    ethnicity,
    CASE WHEN any_use_case_active = 0 THEN NULL ELSE use_case_backup END AS use_case_backup,
    CASE WHEN any_use_case_active = 0 THEN NULL ELSE use_case_business END AS use_case_business,
    CASE WHEN any_use_case_active = 0 THEN NULL ELSE use_case_buying_or_selling END AS use_case_buying_or_selling,
    CASE WHEN any_use_case_active = 0 THEN NULL ELSE use_case_dating END AS use_case_dating,
    CASE WHEN any_use_case_active = 0 THEN NULL ELSE use_case_for_work END AS use_case_for_work,
    CASE WHEN any_use_case_active = 0 THEN NULL ELSE use_case_job_hunting END AS use_case_job_hunting,
    CASE WHEN any_use_case_active = 0 THEN NULL ELSE use_case_long_distance_calling END AS use_case_long_distance_calling,
    CASE WHEN any_use_case_active = 0 THEN NULL ELSE use_case_other END AS use_case_other,
    CASE WHEN any_use_case_active = 0 THEN NULL ELSE use_case_primary END AS use_case_primary,
    interests_automotive,
    interests_dating,
    interests_entertainment,
    interests_finance,
    interests_food,
    interests_health_and_fitness,
    interests_news,
    interests_retail,
    interests_travel
FROM self_reported_massage
