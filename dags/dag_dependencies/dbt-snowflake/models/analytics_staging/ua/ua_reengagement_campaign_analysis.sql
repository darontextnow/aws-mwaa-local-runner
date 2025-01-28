
{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='SURROGATE_KEY',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

/* Re-engagement campaigns have been launched on devices where the app has been installed but has not been registered in 3 months, splitting the group into Group-1 and Group-2, targeting devices in Group 1 and monitoring their impact in comparison to Group 2.
*/
SELECT 
 
  a.adid as adid,
  a.client_type,
  a.installed_at as installed_at,
  {{ dbt_utils.generate_surrogate_key(['adid', 'client_type','installed_at']) }} AS SURROGATE_KEY,
  a.registered_date AS registered_at,
  category as category ,
  Current_timestamp as inserted_at
FROM
(
  SELECT 
    installs.adjust_id as adid,
    installs.client_type as client_type,
    installs.installed_at,
    r.created_at AS registered_date,
    'Group-1' AS Category,
    ROW_NUMBER() OVER(PARTITION BY installs.adjust_id,installs.client_type ORDER BY installs.installed_at DESC) AS rn 
  FROM {{ source('adjust', 'installed_not_registered_in_last_3_months_group1') }}  AS group1
  INNER JOIN {{ ref('adjust_installs') }} installs 
  ON group1.adid=installs.gps_adid 
  LEFT JOIN {{ ref('adjust_registrations') }} r 
  ON r.adjust_id=installs.adjust_id
  AND NVL(r.created_at,'1999-01-01')>installs.installed_at
  WHERE group1.run_date >= '2023-05-09'
  {% if is_incremental() %}
  AND group1.run_date >= (SELECT MAX(INSERTED_AT)::DATE - INTERVAL '2 days' FROM {{ this }})
  {% endif %}



UNION ALL

  SELECT
    installs.adjust_id as adid,
    installs.client_type as client_type,
    installs.installed_at,
    r.created_at AS registered_date,
    'Group-2' AS Category,
    ROW_NUMBER() OVER(PARTITION BY installs.adjust_id,installs.client_type  ORDER BY installs.installed_at DESC) AS rn 
  FROM {{ source('adjust', 'installed_not_registered_in_last_3_months_group2') }} as group2
  INNER JOIN  {{ ref('adjust_installs') }} installs 
  ON group2.adid=installs.gps_adid
  LEFT JOIN {{ ref('adjust_registrations') }} r 
  ON r.adjust_id=installs.adjust_id
  AND NVL(r.created_at,'1999-01-01')>installs.installed_at

  WHERE group2.run_date >= '2023-05-09'
  {% if is_incremental() %}
  AND group2.run_date >= (SELECT MAX(INSERTED_AT)::DATE - INTERVAL '2 days' FROM {{ this }})
  {% endif %}
--and NVL(r.created_at,'')>installs.installed_at


)a

WHERE a.rn=1
