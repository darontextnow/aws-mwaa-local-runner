{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

-- One row contains one user with the most recent cases that he has selected
-- user_profile_use_cases_updates doesn't join users to grab username because the table is nested, so we join users here

WITH user_use_cases AS (
    SELECT
        coalesce(a.username, u.username) AS updated_username,
        updated_at,
        LOWER(SUBSTRING(use_case,13)) as use_case,
        DENSE_RANK() OVER (PARTITION BY updated_username ORDER BY updated_at DESC) AS ranking
     FROM {{ ref('user_profile_use_cases_updates') }} a
     LEFT JOIN {{ ref('analytics_users') }} u USING (user_id_hex)
    WHERE updated_username IS NOT NULL
    )

SELECT DISTINCT
    updated_username AS username,
    updated_at,
    -- format of use case: 'TN_USE_CASE_*'
    LISTAGG(DISTINCT use_case , ', ') WITHIN GROUP (ORDER BY use_case) AS use_cases
 FROM user_use_cases
WHERE ranking = 1
GROUP BY username, updated_at