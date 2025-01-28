{{
    config(
        tags=['daily'],
        materialized='view',
        bind=False
    )
}}


SELECT
    username,
    adid                AS adjust_id,
    client_type,
    app_version         AS client_version,
    created_at,
    installed_at,
    tracker,
    store,
    last_time_spent,
    connection_type,
    is_organic,
    os_version,
    UPPER(country)      AS country_code,
    tracking_limited
FROM {{ ref('sessions_with_pi') }}
JOIN {{ source('adjust', 'apps') }} USING (app_name)