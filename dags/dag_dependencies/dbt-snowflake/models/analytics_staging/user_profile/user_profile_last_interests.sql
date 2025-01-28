{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

-- One row contains one user with the most recent interests that he has selected
-- user_profile_interests_updates doesn't join users to grab username because the table is nested, so we join users here

WITH user_interests AS (
    SELECT
        coalesce(a.username, u.username) AS updated_username,
        updated_at,
        LOWER(SUBSTRING(interest,10)) as interest,
        DENSE_RANK() OVER (PARTITION BY updated_username ORDER BY updated_at DESC) AS ranking
     FROM {{ ref('user_profile_interests_updates') }} a
     LEFT JOIN {{ ref('analytics_users') }} u USING (user_id_hex)
    WHERE updated_username IS NOT NULL
    )

SELECT DISTINCT
    updated_username AS username,
    updated_at,
    -- format of interest: 'INTEREST_*'
    LISTAGG(DISTINCT interest, ', ') WITHIN GROUP (ORDER BY interest) AS interests
 FROM user_interests
WHERE ranking = 1
GROUP BY username, updated_at