{{
    config(
        tags=['daily_features'],
        full_refresh = false,
        materialized='incremental',
        unique_key='date_utc'
    )
}}

WITH user_permissions AS (
    SELECT
        date_utc,
        username,
        concat_ws(
            '_',
            SUBSTR(permission_type, 17),
            RIGHT(event_source, 2),
            SUBSTR(permission_alert_state, 13)
        ) AS permission
    FROM {{ ref('new_user_permissions') }}
    WHERE (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('ds') }}::DATE)
)

SELECT
    date_utc,
    username,
    SUM(CASE WHEN permission = 'CONTACT_OS_SHOWN' THEN 1 ELSE 0 END) AS contact_os_shown,
    SUM(CASE WHEN permission = 'CONTACT_OS_ACCEPTED' THEN 1 ELSE 0 END) AS contact_os_accepted,
    SUM(CASE WHEN permission = 'CONTACT_OS_DENIED' THEN 1 ELSE 0 END) AS contact_os_denied,
    SUM(CASE WHEN permission = 'ANDROID_SETUP_TN_SHOWN' THEN 1 ELSE 0 END) AS android_setup_tn_shown,
    SUM(CASE WHEN permission = 'ANDROID_SETUP_TN_ACCEPTED' THEN 1 ELSE 0 END) AS android_setup_tn_accepted,
    SUM(CASE WHEN permission = 'ANDROID_SETUP_TN_DENIED' THEN 1 ELSE 0 END) AS android_setup_tn_denied,
    SUM(CASE WHEN permission = 'ANDROID_SETUP_OS_SHOWN' THEN 1 ELSE 0 END) AS android_setup_os_shown,
    SUM(CASE WHEN permission = 'ANDROID_SETUP_OS_ACCEPTED' THEN 1 ELSE 0 END) AS android_setup_os_accepted,
    SUM(CASE WHEN permission = 'ANDROID_SETUP_OS_DENIED' THEN 1 ELSE 0 END) AS android_setup_os_denied,
    SUM(CASE WHEN permission = 'MICROPHONE_TN_SHOWN' THEN 1 ELSE 0 END) AS microphone_tn_shown,
    SUM(CASE WHEN permission = 'MICROPHONE_TN_ACCEPTED' THEN 1 ELSE 0 END) AS microphone_tn_accepted,
    SUM(CASE WHEN permission = 'MICROPHONE_TN_DENIED' THEN 1 ELSE 0 END) AS microphone_tn_denied,
    SUM(CASE WHEN permission = 'MICROPHONE_OS_SHOWN' THEN 1 ELSE 0 END) AS microphone_os_shown,
    SUM(CASE WHEN permission = 'MICROPHONE_OS_ACCEPTED' THEN 1 ELSE 0 END) AS microphone_os_accepted,
    SUM(CASE WHEN permission = 'MICROPHONE_OS_DENIED' THEN 1 ELSE 0 END) AS microphone_os_denied,
    SUM(CASE WHEN permission = 'LOCATION_TN_SHOWN' THEN 1 ELSE 0 END) AS location_tn_shown,
    SUM(CASE WHEN permission = 'LOCATION_TN_ACCEPTED' THEN 1 ELSE 0 END) AS location_tn_accepted,
    SUM(CASE WHEN permission = 'LOCATION_TN_DENIED' THEN 1 ELSE 0 END) AS location_tn_denied,
    SUM(CASE WHEN permission = 'LOCATION_OS_SHOWN' THEN 1 ELSE 0 END) AS location_os_shown,
    SUM(CASE WHEN permission = 'LOCATION_OS_ACCEPTED' THEN 1 ELSE 0 END) AS location_os_accepted,
    SUM(CASE WHEN permission = 'LOCATION_OS_DENIED' THEN 1 ELSE 0 END) AS location_os_denied,
    SUM(CASE WHEN permission = 'PHONE_OS_SHOWN' THEN 1 ELSE 0 END) AS phone_os_shown,
    SUM(CASE WHEN permission = 'PHONE_OS_ACCEPTED' THEN 1 ELSE 0 END) AS phone_os_accepted,
    SUM(CASE WHEN permission = 'PHONE_OS_DENIED' THEN 1 ELSE 0 END) AS phone_os_denied
FROM user_permissions
GROUP BY 1, 2
