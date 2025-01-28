{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

SELECT adjust_id AS ext_device_id,
       COALESCE(IDFA, GPS_ADID) AS mobile_ad_id,
       COALESCE(IDFV, ANDROID_ID) AS mobile_vendor_id,
       'ADJUST' AS ext_device_id_source,
       client_type AS client_type,
       device_type AS device_type,
       device_name AS device_name
FROM {{ ref('adjust_installs') }}
