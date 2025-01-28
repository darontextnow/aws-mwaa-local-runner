/*
    App usage features from PP lifecycle events
    meant to replace the LP features being sunset
    features are computed over a window of past 90 days like for LP features
*/
INSERT INTO core.existing_user_trust_score_training_features_part4
SELECT
    username,
    '{{ ds }}'::DATE date_utc,
    COUNT(*) num_applifecyle_events,
    SUM(CASE WHEN supported_tz.tz_code IS NULL AND tz_code IS NOT NULL
            THEN 1 ELSE 0 END) num_applifecycle_events_unsupported_tz,
    SUM(LEAST(600, DATEDIFF('SECONDS', event_time, next_event_time))) total_duration_applifecycle,
    SUM(CASE WHEN platform = 'IOS'
            THEN LEAST(600, DATEDIFF('SECONDS', event_time, next_event_time))
            ELSE 0 END) total_duration_applifecycle_ios,
    SUM(CASE WHEN platform='CP_ANDROID'
            THEN LEAST(600, DATEDIFF('SECONDS', event_time, next_event_time))
            ELSE 0 END) total_duration_applifecycle_android,
    MEDIAN(DATEDIFF('SECONDS', event_time, next_event_time)) median_applifecycle_duration,
    SUM(CASE WHEN supported_tz.tz_code IS NULL AND tz_code IS NOT NULL
            THEN LEAST(600, DATEDIFF('SECONDS', event_time, next_event_time))
            ELSE 0 END) total_duration_applifecycle_unsupported_tz,
    SUM(CASE WHEN high_risk_tz.tz_code IS NOT NULL
            THEN LEAST(600, DATEDIFF('SECONDS', event_time, next_event_time))
            ELSE 0 END) total_duration_applifecycle_highrisk_tz,
    SUM(
        CASE WHEN supported_tz.tz_code IS NULL AND tz_code IS NOT NULL
             AND event_time > '{{ ds }}'::DATE - INTERVAL '2 weeks'
             THEN LEAST(600, DATEDIFF('SECONDS', event_time, next_event_time))
        ELSE 0 END) total_duration_applifecycle_unsupported_tz_past_2weeks,
    SUM(
        CASE WHEN high_risk_tz.tz_code IS NOT NULL
             AND event_time > '{{ ds }}'::DATE - INTERVAL '2 weeks'
             THEN LEAST(600, DATEDIFF('SECONDS', event_time, next_event_time))
        ELSE 0 END) total_duration_applifecycle_highrisk_tz_past_2weeks,
    ARRAY_AGG(DISTINCT CASE WHEN supported_tz.tz_code IS NULL THEN tz_code ELSE NULL END) unsupported_tz_list,
    COUNT(DISTINCT CASE WHEN supported_tz.tz_code IS NULL
        THEN DATE(event_time)
        ELSE NULL END) num_days_unsupported_tz,
    SUM(CASE WHEN LOWER(client_country_code) NOT IN ('us','ca','mx')
            THEN LEAST(600, DATEDIFF('SECONDS', event_time, next_event_time))
            ELSE 0 END) total_duration_applifecycle_unsupported_client_country,
    SUM(CASE WHEN LOWER(COUNTRTY_ISO_CODE) NOT IN ('us','ca','mx')
            THEN LEAST(600, DATEDIFF('SECONDS', event_time, next_event_time))
            ELSE 0 END) total_duration_applifecycle_unsupported_ip_country,
    SUM(CASE WHEN (LOWER(COUNTRTY_ISO_CODE) NOT IN ('us','ca','mx'))
                 AND (event_time > '{{ ds }}'::DATE - INTERVAL '2 weeks')
            THEN LEAST(600, DATEDIFF('SECONDS', event_time, next_event_time))
            ELSE 0 END) total_duration_applifecycle_unsupported_ip_country_past_2weeks,
    SUM(CASE WHEN COALESCE(hosting,vpn,proxy) = TRUE
            THEN LEAST(600, DATEDIFF('SECONDS', event_time, next_event_time))
            ELSE 0 END) total_duration_applifecycle_privacy_ip,
    SUM(CASE WHEN (COALESCE(hosting,vpn,proxy) = TRUE)
                AND (event_time > '{{ ds }}'::DATE - INTERVAL '2 weeks')
            THEN LEAST(600, DATEDIFF('SECONDS', event_time, next_event_time))
            ELSE 0 END) total_duration_applifecycle_privacy_ip_past_2weeks,
    SUM(CASE WHEN language_code NOT IN ('en','es','fr')
            THEN LEAST(600, DATEDIFF('SECONDS', event_time, next_event_time))
            ELSE 0 END) total_duration_applifecycle_unsupported_language,
    COUNT(DISTINCT CASE WHEN len(adjust_id)>0 THEN adjust_id ELSE NULL END) num_adjust_devices_applifecycle,
    SUM(CASE WHEN anon_install.adjust_id IS NOT NULL
            THEN LEAST(600, DATEDIFF('SECONDS', event_time, next_event_time))
            ELSE 0 END) total_duration_applifecycle_anon_installed_recent,
    COUNT(DISTINCT CASE WHEN anon_install.adjust_id IS NOT NULL
            THEN anon_install.adjust_id
            ELSE NULL END) num_devices_applifecycle_anon_installed_recent
FROM (
    SELECT
        *,
        CASE WHEN platform = 'CP_ANDROID' AND brand = 'BRAND_2NDLINE' THEN '2L_ANDROID'
             WHEN platform = 'CP_ANDROID' AND brand = 'BRAND_TEXTNOW' THEN 'TN_ANDROID'
             WHEN platform = 'IOS' THEN 'TN_IOS_FREE'
        ELSE NULL
        END AS client_type
    FROM analytics_staging.applifecycle_events
    WHERE
        (event='APP_LIFECYCLE_FOREGROUNDED')
        AND (next_event='APP_LIFECYCLE_BACKGROUNDED')
        AND (date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '90 DAYS' AND '{{ ds }}'::DATE)
        AND (DATE(event_time) BETWEEN '{{ ds }}'::DATE - INTERVAL '90 DAYS' AND '{{ ds }}'::DATE)
) al
JOIN analytics_staging.existing_user_features_user_snapshot USING (username)
LEFT JOIN trust_safety.supported_tz USING (tz_code)
LEFT JOIN trust_safety.high_risk_tz USING (tz_code)
LEFT JOIN core.ip_geo_info_latest USING (ip_address)
LEFT JOIN core.ip_privacy_update_latest USING (ip_address)
LEFT JOIN (
    SELECT DISTINCT adid AS adjust_id
    FROM adjust.installs
    WHERE
        (tracker_name ILIKE 'untrusted%anonymous%')
        AND (DATE(installed_at) BETWEEN  '{{ ds }}'::DATE - INTERVAL '180 DAYS' AND '{{ ds }}'::DATE)
) anon_install USING (adjust_id)
GROUP BY 1
