{{
    config(
        tags=['daily_features'],
        full_refresh = false,
        materialized='incremental',
        unique_key='username'
    )
}}

SELECT DISTINCT
    username,
    u.created_at AS user_created_at,
    user_set_id,
    us.created_at AS user_set_created_at,
    CASE WHEN u.created_at <= us.first_paid_device_date THEN FALSE ELSE TRUE END AS is_organic --if username created_at <= user set first paid device date then organic else paid
FROM {{ ref('analytics_users') }} u
LEFT JOIN {{ ref('user_sets') }} us USING (user_set_id)
WHERE (u.created_at BETWEEN {{ var('ds') }}::DATE - INTERVAL '5 DAYS' AND {{ var('ds') }}::DATE)
