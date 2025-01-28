{{
    config(
        tags=['daily'],
        materialized='incremental',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

WITH unique_installs AS (
    SELECT a.app_id,
           i.adid,
           MIN(i.INSTALLED_AT) AS installed_at,
           NVL(MIN(r.CREATED_AT), SYSDATE()) AS valid_until
    FROM {{ ref('installs_with_pi') }} i
    LEFT JOIN {{ ref('reattribution_with_pi') }} r ON (i.adid = r.adid) AND (i.app_name = r.app_name)
    JOIN {{ source('adjust', 'apps') }} a ON (i.app_name = a.app_name)
    GROUP BY 1, 2
)
SELECT stage.*
FROM (
    SELECT DISTINCT 
        app_id,
        adid,
        u.installed_at,
        s.date_utc AS active_date,
        DATEDIFF(DAY, u.installed_at, s.date_utc) AS day_from_install
    FROM {{ source('dau', 'user_device_master') }} s
    JOIN {{ source('adjust', 'apps') }} USING (client_type)
    JOIN unique_installs u USING (app_id, adid)
    WHERE
        (s.date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '4 DAY' AND {{ var('ds') }}::DATE)
        AND (s.date_utc < valid_until)
) stage
LEFT JOIN {{ this }} a ON
    (stage.app_id = a.app_id)
    AND (stage.adid = a.adid)
    AND (stage.day_from_install = a.day_from_install)
WHERE (a.adid IS NULL)
