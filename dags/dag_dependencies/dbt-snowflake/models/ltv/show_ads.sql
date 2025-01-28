{{
    config(
        tags=['daily'],
        materialized='incremental',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

WITH from_registrations AS (
    SELECT app_id, adid, MIN(expiry) AS show_ad_date
    FROM {{ source('core', 'users') }} u
    JOIN {{ ref('registrations_1') }} r USING (username)
    JOIN {{ source('adjust', 'apps') }} a USING (client_type)
    LEFT JOIN {{ this }} AS sa USING (app_id, adid)
    WHERE
        (u.created_at >= {{ var('ds') }}::DATE - INTERVAL '2 DAYS')
        AND (r.cohort_utc >= {{ var('ds') }}::DATE - INTERVAL '2 DAYS')
        AND (sa.adid IS NULL)
        AND (r.adid IS NOT NULL)
    GROUP BY 1, 2
)

SELECT * FROM from_registrations
UNION ALL SELECT a.app_id, s.adid, MIN(expiry) AS show_ad_date
FROM {{ source('core', 'users') }} u
JOIN {{ ref('sessions_with_pi') }} s USING (username)
JOIN {{ source('adjust', 'apps') }} a USING (app_name)
LEFT JOIN {{ this }} AS sa ON (a.app_id = sa.app_id) AND (s.adid = sa.adid)
LEFT JOIN from_registrations r ON (a.app_id = r.app_id) AND (s.adid = r.adid)
WHERE
    (s.created_at >= {{ var('ds') }}::DATE - INTERVAL '2 DAYS')
    AND (sa.adid IS NULL) -- don't include rows already in table
    AND (r.adid IS NULL) -- don't include rows being added by from_registrations
GROUP BY 1, 2
