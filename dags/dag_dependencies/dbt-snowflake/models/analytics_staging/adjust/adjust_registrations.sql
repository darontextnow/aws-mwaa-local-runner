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
    click_time,
    tracker,
    store,
    impression_based,
    is_organic,
    match_type,
    device_type,
    device_name,
    os_name,
    os_version,
    UPPER(country)      AS country_code
FROM {{ source('adjust', 'registrations') }}
JOIN {{ source('adjust', 'apps') }} USING (app_name)
WHERE environment = 'production'