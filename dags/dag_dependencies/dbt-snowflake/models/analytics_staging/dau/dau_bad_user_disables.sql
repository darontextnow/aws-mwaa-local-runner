{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='username',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH non_dormant_disables AS (
    SELECT
        u.username,
        MIN(DATE(u.timestamp)) AS disabled_date
    FROM {{ source('core', 'disabled') }} d
    INNER JOIN {{ source('core', 'users') }} u ON d.global_id = u.user_id_hex
    WHERE
        requesting_service_name != 'Compliance'
        AND disable_reason NOT IN ('CCPA Request Deletion', 'abandoned-account', 'dormant-60-days-non-paying-account')
        AND disable_reason NOT LIKE '%-since-%'
        AND u.account_status NOT IN ('ENABLED', 'LEGACY', 'CAPTCHA_REQUIRED')
    GROUP BY 1
),
untracked_disables AS (
    SELECT
        username,
        MIN(DATE(u.timestamp)) AS disabled_date
    FROM {{ source('core', 'users') }} u
    LEFT JOIN {{ source('core', 'disabled') }} d ON u.user_id_hex = d.global_id
    WHERE
        u.account_status NOT IN ('ENABLED', 'LEGACY', 'CAPTCHA_REQUIRED')
        AND global_id IS NULL
        AND timestamp < {{ var('data_interval_start') }}::TIMESTAMP - INTERVAL '1 hour' -- since core.disabled gets updated only hourly some may not show up yet
    GROUP BY 1
)

SELECT
    username,
    disabled_date
FROM non_dormant_disables
UNION ALL SELECT
    username,
    disabled_date
FROM untracked_disables
