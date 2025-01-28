{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='surrogate_key',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

SELECT
    i.surrogate_key,
    i.event_received_date_utc,
    i.adjust_id,
    i.app_id,
    i.client_type,
    i.app_name,
    i.client_version,
    i.installed_at,
    i.click_time,
    COALESCE(t.tracker, i.tracker) AS tracker,
    i.store,
    i.impression_based,
    COALESCE(t.is_organic, i.is_organic) AS is_organic,
    i.is_untrusted,
    COALESCE(t.match_type, i.match_type) AS match_type,
    i.device_type,
    i.device_name,
    i.os_name,
    i.os_version,
    i.sdk_version,
    i.region_code,
    i.country_code,
    i.country_subdivision,
    i.city,
    i.postal_code,
    i.language,
    i.ip_address,
    i.tracking_limited,
    i.deeplink,
    i.timezone,
    i.connection_type,
    i.idfa,
    i.idfv,
    i.gps_adid,
    i.android_id
FROM {{ ref('adjust_installs_stage') }} AS i
LEFT JOIN {{ ref('ua_tatari_streaming_installs') }} AS t ON
    (t.surrogate_key = i.surrogate_key)
    AND (t.adjust_id = i.adjust_id)
    AND (t.installed_at <= t.impressed_at + INTERVAL '7 DAY')  -- 7-day attribution window for Tatari Streaming
