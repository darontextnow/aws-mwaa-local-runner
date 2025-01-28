{{
    config(
        tags=['daily'],
        materialized='incremental',
        snowflake_warehouse='PROD_WH_MEDIUM',
        pre_hook=[
            "DELETE FROM {{ this }} WHERE (created_at >= {{ var('data_interval_start') }}::TIMESTAMP - INTERVAL '25 HOUR')"
        ]
    )
}}

WITH nps AS (
    SELECT DISTINCT 
        username,
        client_type,
        client_version,
        MAX(created_at) OVER (PARTITION BY client_type, username, client_version) AS created_at,
        LAST_VALUE(score IGNORE NULLS) OVER
            (PARTITION BY client_type, username, client_version
             ORDER BY created_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS score,
        LAST_VALUE(reason IGNORE NULLS) OVER
            (PARTITION BY client_type, username, client_version
             ORDER BY created_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS reason,
        LAST_VALUE(final_action IGNORE NULLS) OVER
            (PARTITION BY client_type, username, client_version
             ORDER BY created_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS final_action,
        LAST_VALUE(store_rating_action IGNORE NULLS) OVER
            (PARTITION BY client_type, username, client_version
             ORDER BY created_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS store_rating_action
    FROM {{ ref('nps_combined_sources') }}
    WHERE 
        (created_at >= {{ var('data_interval_start') }}::TIMESTAMP - INTERVAL '2 DAYS')
        AND (username IS NOT NULL)
)

SELECT
    nps.created_at,
    nps.username,
    users.created_at AS installed_at,
    nps.client_type,
    nps.client_version,
    nps.score,
    nps.reason,
    nps.store_rating_action,
    nps.final_action,
    CASE
        WHEN subs.username IS NOT NULL THEN 'active_wireless'
        WHEN users.created_at < nps.created_at - INTERVAL '1 WEEK'
            AND users.expiry + INTERVAL '1 DAY' >= nps.created_at THEN 'premium'
        ELSE 'free'
    END AS user_status,
    regs.is_organic,
    NULL::FLOAT AS positivity,
    NULL AS country_code,
    REGEXP_LIKE(reason, '.*(call|drop|service|connection|connectivity|quality|reception|signal).*', 'i') AS is_reason_about_calls,
    REGEXP_LIKE(reason, '.*(slow| load|^load|speed|lag).*', 'i') AS is_reason_about_speed
FROM nps
LEFT JOIN {{ source('core', 'users') }} users ON (nps.username = users.username)
LEFT JOIN {{ ref('registrations_1') }} regs ON (nps.username = regs.username)
LEFT JOIN {{ ref('subscriptions_snapshot') }} subs ON
    (nps.username = subs.username)
    AND (subs.status IN ('ACTIVE','THROTTLED'))
    AND (nps.created_at::DATE = subs.DATE_UTC)
WHERE (nps.created_at >= {{ var('data_interval_start') }}::TIMESTAMP - INTERVAL '25 HOUR')