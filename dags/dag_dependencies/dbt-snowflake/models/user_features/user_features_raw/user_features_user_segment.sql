{{
    config(
        tags=['daily_features'],
        full_refresh = false,
        materialized='incremental',
        unique_key='date_utc'
    )
}}

SELECT
    date_utc,
    username,
    COALESCE(sub_type, 'Non-Sub') AS sub_type,
    ad_upgrade_type,
    phone_num_upgrade_type,
    consumable_type
FROM {{ ref('user_features_active_device') }}
LEFT JOIN {{ ref('user_segment_username_daily_tn_type') }} USING (date_utc. username)
WHERE (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '5 DAYS' AND {{ var('ds') }}::DATE)
