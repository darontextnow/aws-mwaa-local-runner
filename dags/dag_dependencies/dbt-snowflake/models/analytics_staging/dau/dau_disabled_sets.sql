{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='user_set_id',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH disabled_set_info as (
    SELECT
        us.set_uuid,
        --logic for disabled_date assumes user typically will not be re-enabled after being disabled.
        MIN(d.disabled_date) AS set_disabled_date --using MIN as occasionally set may be disabled more than once.
    FROM {{ ref('dau_bad_user_disables') }} d
    INNER JOIN {{ source('dau', 'user_set') }} us ON us.username = d.username
    GROUP BY 1
)

SELECT DISTINCT
    set_disabled_date AS disabled_date,
    disabled.set_uuid AS user_set_id,
    COALESCE (LAST_VALUE(client_type) OVER
        (PARTITION BY dau.user_set_id ORDER BY date_utc ROWS BETWEEN unbounded preceding AND unbounded following),
        'Never_Active_on_Mobile') AS client_type
FROM disabled_set_info AS disabled
LEFT JOIN {{ ref('dau_user_set_active_days') }} AS dau
    ON (disabled.set_uuid = dau.user_set_id AND date_utc <= set_disabled_date)
ORDER BY 1
