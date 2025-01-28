{{
    config(
        tags=['daily'],
        materialized='view',
        bind=False,
        unique_key='date_utc || username'
    )
}}

-- web_users and other_users are excluded

SELECT
    date_utc,
    username,
    rfm_segment
FROM {{ ref('user_segment_user_set_daily_rfm') }} AS rfm
JOIN {{ source('dau', 'user_set') }} AS u ON (u.set_uuid = rfm.user_set_id)