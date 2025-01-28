{{
    config(
        tags=['weekly'],
        materialized='table',
        snowflake_warehouse='PROD_WH_SMALL'
    ) 
}}

-- * File: personas_trust_score.sql
-- * Author: Hao Zhang
-- * Purpose:
--   Aggregates trust scores from multiple sources to compute the highest
--   likelihood of disablement per user in the marketing personas universe.

-- * Step Overview:
-- Step 1. Retrieve trust scores for new users from the first sources.
-- Step 2. Retrieve trust scores for new users from the second sources.
-- Step 3. Retrieve trust scores for existing users.
-- Step 4. Retrieve the most recent trust scores for existing users from the last 30 days.
-- Step 5. Combine all sources and aggregate the highest likelihood of disablement per user.
-- Step 6: Compute the maximum likelihood of disablement for each user

WITH new_user_trust_scores AS (
-- Step 1: Retrieve trust scores for new users from the first source
    SELECT a.username, likelihood_of_disable
    FROM {{ source('analytics', 'new_user_trust_scores') }} AS a
    INNER JOIN {{ ref('persona_universe') }} AS b ON (a.username = b.username)
),

-- Step 2: Retrieve trust scores for existing users
existing_user_trust_scores AS (
    SELECT a.username, likelihood_of_disable
    FROM {{ source('analytics', 'existing_user_trust_scores') }} AS a
    INNER JOIN {{ ref('persona_universe') }} AS b ON (a.username = b.username)
),

-- Step 4: Retrieve the most recent trust scores for existing users (last 30 days)
existing_user_trust_scores_latest_30days AS (
    SELECT a.username, likelihood_of_disable
    FROM {{ source('analytics', 'existing_user_trust_scores_latest_30days') }} AS a
    INNER JOIN {{ ref('persona_universe') }} AS b ON (a.username = b.username)
),

-- Step 5: Combine all trust score sources into a unified table
trust_scores AS (
    SELECT * FROM new_user_trust_scores
    UNION ALL SELECT * FROM existing_user_trust_scores
    UNION ALL SELECT * FROM existing_user_trust_scores_latest_30days
)

-- Step 6: Compute the maximum likelihood of disablement for each user
SELECT username, MAX(likelihood_of_disable) AS likelihood_of_disable
FROM trust_scores
GROUP BY 1
