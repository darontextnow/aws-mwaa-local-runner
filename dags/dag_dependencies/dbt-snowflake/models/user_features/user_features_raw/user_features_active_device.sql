{{
    config(
        tags=['daily_features'],
        full_refresh = false,
        materialized='incremental',
        unique_key='date_utc'
    )
}}

WITH active_users AS (
    SELECT DISTINCT date_utc, username, client_type, adid
    FROM {{ source('dau', 'user_device_master') }}
    WHERE
        (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '5 DAYS' AND {{ var('ds') }}::DATE)
        AND (client_type IN ('TN_ANDROID', '2L_ANDROID', 'TN_IOS_FREE'))
),
primary_key AS (
    SELECT DISTINCT date_utc, username FROM active_users
),
client_type_info AS (
    SELECT 
        date_utc,
        username,
        MAX(CASE WHEN client_type = 'TN_ANDROID' THEN 1 ELSE 0 END) AS active_android,
        MAX(CASE WHEN client_type = '2L_ANDROID' THEN 1 ELSE 0 END) AS active_sec_android,
        MAX(CASE WHEN client_type = 'TN_IOS_FREE' THEN 1 ELSE 0 END) AS active_ios
    FROM active_users
    GROUP BY 1, 2
    ORDER BY 1, 2
),
android_device_info AS (
    SELECT 
        date_utc,
        username,
        LISTAGG(adid, ', ') WITHIN GROUP (ORDER BY installed_at) AS android_adid,
        LISTAGG(device_type, ', ') WITHIN GROUP (ORDER BY installed_at) AS android_device_type,
        LISTAGG(device_name, ', ') WITHIN GROUP (ORDER BY installed_at) AS android_device_name
    FROM active_users a
    LEFT JOIN {{ ref('adjust_installs') }} i ON (a.adid = i.adjust_id) AND (a.client_type = i.client_type)
    WHERE (a.client_type = 'TN_ANDROID')
    GROUP BY 1, 2
),
sec_android_device_info AS (
    SELECT
        date_utc,
        username,
        LISTAGG(adid, ', ') WITHIN GROUP (ORDER BY installed_at) AS sec_android_adid,
        LISTAGG(device_type, ', ') WITHIN GROUP (ORDER BY installed_at) AS sec_android_device_type,
        LISTAGG(device_name, ', ') WITHIN GROUP (ORDER BY installed_at) AS sec_android_device_name
    FROM active_users a
    LEFT JOIN {{ ref('adjust_installs') }} i ON (a.adid = i.adjust_id) AND (a.client_type = i.client_type)
    WHERE (a.client_type = '2L_ANDROID')
    GROUP BY 1, 2
),

ios_device_info AS (
    SELECT date_utc,
        username,
        LISTAGG(adid, ', ') WITHIN GROUP (ORDER BY installed_at) AS ios_adid,
        LISTAGG(device_type, ', ') WITHIN GROUP (ORDER BY installed_at) AS ios_device_type,
        LISTAGG(device_name, ', ') WITHIN GROUP (ORDER BY installed_at) AS ios_device_name
    FROM active_users a
    LEFT JOIN {{ ref('adjust_installs') }} i ON (a.adid = i.adjust_id) AND (a.client_type = i.client_type)
    WHERE (a.client_type = 'TN_IOS_FREE')
    GROUP BY 1, 2
)

SELECT *
FROM primary_key
LEFT JOIN client_type_info USING (date_utc, username)
LEFT JOIN android_device_info USING (date_utc, username)
LEFT JOIN sec_android_device_info USING (date_utc, username)
LEFT JOIN ios_device_info USING (date_utc, username)
