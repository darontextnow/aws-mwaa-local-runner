{{
    config(
        tags=['daily'],
        materialized='incremental',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

SELECT DISTINCT
    s.username,
    u.created_at,
    DATEDIFF(DAY, u.created_at, s.date_utc) AS day_from_registration,
    s.date_utc AS active_date
FROM {{ source('dau', 'user_device_master') }} s
JOIN {{ source('core', 'users') }} u USING (username)
LEFT JOIN (
    SELECT DISTINCT username, day_from_registration
    FROM {{ this }}
    {% if is_incremental() or target.name == 'dev' %}
        WHERE (active_date BETWEEN {{ var('ds') }}::DATE - INTERVAL '4 DAY' AND {{ var('ds') }}::DATE)
    {% endif %}
) a ON (s.username = a.username) AND (DATEDIFF(DAY, u.created_at, s.date_utc) = a.day_from_registration)
WHERE
    (s.date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '4 DAY' AND {{ var('ds') }}::DATE)
    AND (a.username IS NULL) --don't include rows already added to target table
    AND (u.created_at IS NOT NULL)
