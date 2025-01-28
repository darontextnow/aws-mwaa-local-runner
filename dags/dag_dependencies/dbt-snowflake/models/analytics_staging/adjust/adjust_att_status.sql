{{
    config(
        tags=['daily'],
        materialized='table'
    )
}}

WITH latest_changes AS (

    SELECT adid,
           app_id,
           app_name,
           MAX(created_at) AS latest_change_ts
    FROM adjust.att_events
    WHERE environment = 'production'
    GROUP BY 1, 2, 3

)

SELECT e.adid AS adjust_id,
       a.client_type,
       app_version AS client_version,
       os_version,
       sdk_version,
       tracker,
       created_at,
       DECODE(att_status,
           0, 'undetermined',
           1, 'restricted',
           2, 'denied',
           3, 'authorized',
           'unknown') AS att_status
FROM latest_changes c
JOIN adjust.att_events e
  ON c.adid = e.adid
 AND c.app_id = e.app_id
 AND c.app_name = e.app_name
 AND c.latest_change_ts = e.created_at
 AND e.environment = 'production'
JOIN adjust.apps a
  ON c.app_name = a.app_name
