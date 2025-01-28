-- This model runs with the dbt features run since only features downstream use this data for lifecycle events
{{
    config(
        tags=['daily_features'],
        full_refresh=false,
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

SELECT
    username,
    instance_id,
    event_time,
    date_utc,
    created_at,
    event_id,
    platform,
    brand,
    idfv,
    android_id,
    adjust_id,
    user_agent,
    client_version,
    client_country_code,
    language_code,
    tz_code,
    ip_address,
    event,
    LEAD(event_time, 1) OVER(PARTITION BY username, instance_id ORDER BY event_time) next_event_time,
    LEAD(event, 1) OVER(PARTITION BY username, instance_id ORDER BY event_time) next_event
FROM (
    SELECT
        "client_details.client_data.user_data.username" username,
        instance_id,
        date_utc,
        event_id,
        created_at,
        "client_details.client_data.client_platform" platform,
        "client_details.client_data.brand" brand,
        "client_details.ios_bonus_data.idfv" idfv,
        "client_details.android_bonus_data.android_id" android_id,
        NVL("client_details.android_bonus_data.adjust_id","client_details.ios_bonus_data.adjust_id") adjust_id,
        "client_details.client_data.user_agent" user_agent,
        "client_details.client_data.client_version" client_version,
        "client_details.client_data.country_code" client_country_code,
        "client_details.client_data.language_code" language_code,
        "client_details.client_data.tz_code" tz_code,
        "client_details.client_data.client_ip_address" ip_address,
        "payload.app_lifecycle" event,
        DATEADD('seconds', create_time_server_offset, created_at) event_time
    FROM {{ source('party_planner_realtime', 'app_lifecycle_changed') }}
    WHERE (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '14 DAYS' AND {{ var('ds') }}::DATE)
)
