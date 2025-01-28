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
    is_organic,
    device_type,
    device_name,
    os_name,
    os_version,
    sdk_version,
    UPPER(country)      AS country_code
FROM {{ source('adjust', 'logins_with_pi') }}
JOIN {{ source('adjust', 'apps')}} USING (app_name)