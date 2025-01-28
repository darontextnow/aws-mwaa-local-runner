{{
    config(
        tags=['weekly'],
        materialized='table',
        snowflake_warehouse='PROD_WH_SMALL'
    ) 
}}


-- * File: persona_email_domain.sql
-- * Author: Hao Zhang
-- * Purpose:
--   This query generates a dataset with user email and domain information,
--   categorizing email domains into education or non-education domains.
--
-- * Step Overview:
--   - Step 1: Extract email and domain details for each user.
--   - Step 2: Identify and categorize domains into education or non-education.
--   - Step 3: Output the enriched dataset with username, email, domain, and categorization.

WITH email_details AS (
-- Step 1: Extract email and domain details for each user
    SELECT DISTINCT
        p.username,
        u.email,
        SPLIT_PART(u.email, '@', 2) AS email_domain,
        CASE -- Categorize domains into education or non-education
            WHEN split_part(u.email, '@', 2) LIKE '%edu'
                THEN 'EDUCATION_DOMAIN'
            ELSE 'NON_EDUCATION_DOMAIN'
        END AS is_education_domain
    FROM {{ ref('persona_universe') }} AS p
    LEFT JOIN {{ source('core', 'users') }} AS u ON (p.username = u.username)
)

-- Step 2: Output the enriched dataset
SELECT * FROM email_details
