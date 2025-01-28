{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}


SELECT
    i.adjust_id,
    i.app_id,
    i.client_type,
    i.app_name,
    i.client_version,
    i.installed_at,
    i.click_time,
    i.tracker,
    i.store,
    i.impression_based,
    i.is_organic,
    i.is_untrusted,
    i.match_type,
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
    i.tracking_limited,
    i.deeplink,
    i.timezone,
    i.connection_type,
    s.score AS trust_score
FROM {{ ref('adjust_installs') }} AS i
LEFT JOIN {{ source('fraud_alerts', 'installs') }} AS s USING (adjust_id)
WHERE COALESCE(s.score, 1.0) >= 0.8  -- default score is 1.0
  AND NOT i.is_untrusted
