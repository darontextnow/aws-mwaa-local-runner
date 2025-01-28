{{
    config(
        tags=['daily'],
        materialized='view',
        unique_key='date_utc'
    )
}}

WITH user_level_info AS (
    SELECT DISTINCT
        d.date_utc,
        u.username,
        u.user_id_hex,
        d.client_type
    FROM {{ source('core', 'users') }} u
    LEFT JOIN {{ source('dau', 'user_device_master') }} d ON u.username = d.username
    WHERE account_status = 'ENABLED'
)
SELECT
    s.date_utc,
    starttime,
    endtime,
    username,
    user_id_hex,
    client_type,
    subscriberid,
    subscriberip,
    uuid,
    billing_subscription_id,
    plan,
    servname,
    throttled,
    current_throttle_level,
    suspended,
    tosviolation,
    protocol,
    txbytes,
    rxbytes,
    totalbytes
FROM {{ source('core', 'sandvine_app_usage') }} s
LEFT JOIN user_level_info u ON (u.user_id_hex = s.uuid) AND (s.date_utc = u.date_utc)
ORDER BY 1