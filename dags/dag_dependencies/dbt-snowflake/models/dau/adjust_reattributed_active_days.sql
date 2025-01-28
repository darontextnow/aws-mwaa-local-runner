{{
    config(
        tags=['daily'],
        materialized='incremental',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

WITH unique_reattribution AS (
    SELECT 
        a.app_id,
        adid,
        created_at,
        created_at AS valid_from,
        NVL(LEAD(created_at) OVER (PARTITION BY a.app_id, adid ORDER BY created_at), SYSDATE()) AS valid_until
    FROM {{ ref('reattribution_with_pi') }} r
    JOIN {{ source('adjust', 'apps') }} a USING (APP_NAME)
)
SELECT stage.*
FROM (
    SELECT DISTINCT
        app_id,
        adid,
        r.created_at,
        s.date_utc AS active_date,
        DATEDIFF(day, r.created_at, s.date_utc) AS day_from_reattributed
    FROM {{ source('dau', 'user_device_master') }} s
    JOIN {{ source('adjust', 'apps') }} USING (client_type)
    JOIN unique_reattribution r USING (app_id, adid)
    WHERE
        (s.date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '4 DAY' AND {{ var('ds') }}::DATE)
        AND (s.date_utc BETWEEN valid_from AND valid_until)
) stage
LEFT JOIN {{ this }} a ON
    (stage.app_id = a.app_id)
    AND (stage.adid = a.adid)
    AND (stage.created_at = a.created_at)
    AND (stage.day_from_reattributed = a.day_from_reattributed)
WHERE (a.adid IS NULL)
