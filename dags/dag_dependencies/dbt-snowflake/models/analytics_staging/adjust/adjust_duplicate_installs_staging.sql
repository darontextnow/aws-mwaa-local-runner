{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

-- staging table used by sessions_with_pi, reattributions_with_pi, and installs_with_pi models.
-- selects all dup installs (with same adid and app_name) and numbers each row by created_at date
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY adid, app_id ORDER BY created_at, environment NULLS LAST) AS nth_install
FROM (
    SELECT i.*
    FROM {{ ref('adjust_installs_with_pi_staging') }} i
    JOIN {{ source('adjust', 'apps') }} a ON (i.app_name = a.app_name)
    JOIN (
        SELECT adid, app_name FROM {{ ref('adjust_installs_with_pi_staging') }} GROUP BY 1, 2 HAVING (COUNT(*) > 1)
    ) dupes ON (i.adid = dupes.adid) AND (i.app_name = dupes.app_name)
)