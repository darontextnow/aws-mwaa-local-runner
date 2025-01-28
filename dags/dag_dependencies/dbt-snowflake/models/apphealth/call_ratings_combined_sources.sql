{{
    config(
        tags=['daily'],
        materialized='view',
    )
}}

SELECT
    date_utc,
    created_date,
    {{ pp_normalized_client_type() }} AS client_type,
    "client_details.android_bonus_data.android_id" AS android_id,
    COALESCE("client_details.android_bonus_data.adjust_id", "client_details.ios_bonus_data.adjust_id") AS adjust_id,
    "client_details.android_bonus_data.firebase_id" AS firebase_id,
    "client_details.android_bonus_data.google_analytics_unique_id" AS google_analytics_unique_id,
    "client_details.android_bonus_data.battery_level" AS battery_level,
    "client_details.android_bonus_data.bluetooth_mac_address" AS bluetooth_mac_address,
    "client_details.android_bonus_data.board_name" AS board_name,
    "client_details.android_bonus_data.bootloader_version" AS bootloader_version,
    "client_details.android_bonus_data.brand_name" AS brand_name,
    "client_details.android_bonus_data.build_fingerprint" AS build_fingerprint,
    "client_details.android_bonus_data.build_id" AS build_id,
    "client_details.android_bonus_data.device_id" AS device_id,
    "client_details.android_bonus_data.device_name" AS device_name,
    "client_details.android_bonus_data.google_play_services_advertising_id" AS google_play_services_advertising_id,
    "client_details.android_bonus_data.hardware_name" AS hardware_name,
    "client_details.android_bonus_data.iccid" AS iccid,
    "client_details.android_bonus_data.is_user_a_monkey" AS is_user_a_monkey,
    "client_details.android_bonus_data.manufacturer_name" AS manufacturer_name,
    COALESCE("client_details.android_bonus_data.model_name", "client_details.ios_bonus_data.model_name") AS model_name,
    "client_details.android_bonus_data.os_base" AS os_base,
    "client_details.android_bonus_data.os_codename" AS os_codename,
    COALESCE("client_details.android_bonus_data.os_version", "client_details.ios_bonus_data.os_version") AS os_version,
    COALESCE("client_details.android_bonus_data.product_name", "client_details.ios_bonus_data.product_name") AS product_name,
    "client_details.android_bonus_data.radio_version" AS radio_version,
    COALESCE("client_details.android_bonus_data.screen_height", "client_details.ios_bonus_data.screen_height") AS screen_height,
    COALESCE("client_details.android_bonus_data.screen_width", "client_details.ios_bonus_data.screen_width") AS screen_width,
    "client_details.android_bonus_data.sdk_version" AS sdk_version,
    "client_details.android_bonus_data.uptime" AS uptime,
    "client_details.android_bonus_data.wifi_mac_address" AS wifi_mac_address,
    "client_details.client_data.brand" AS brand,
    "client_details.client_data.checkout_workflow_data.is_gifted_device" AS is_gifted_device,
    "client_details.client_data.checkout_workflow_data.stripe_token" AS stripe_token,
    "client_details.client_data.client_ip_address" AS client_ip_address,
    "client_details.client_data.client_platform" AS client_platform,
    "client_details.client_data.client_port" AS client_port,
    "client_details.client_data.client_version" AS client_version,
    "client_details.client_data.country_code" AS country_code,
    "client_details.client_data.language_code" AS language_code,
    "client_details.client_data.client_calling_sdk_version" AS client_calling_sdk_version,
    "client_details.client_data.tz_code" AS tz_code,
    "client_details.client_data.user_agent" AS user_agent,
    "client_details.client_data.user_data.date_of_birth" AS date_of_birth,
    "client_details.client_data.user_data.email" AS email,
    "client_details.client_data.user_data.email_status" AS email_status,
    "client_details.client_data.user_data.first_name" AS first_name,
    "client_details.client_data.user_data.last_name" AS last_name,
    "client_details.client_data.user_data.username" AS username,
    "client_details.ios_bonus_data.idfa" AS idfa,
    "client_details.ios_bonus_data.idfv" AS idfv,
    create_time_server_offset,
    created_at,
    event_id,
    instance_id,
    proto_class,
    user_id,
    user_id_hex,
    NULL AS platform,
    "payload.call_id" AS call_id,
    "payload.rating" AS call_rating,
    "payload.call_problem" AS call_problem,
    --"payload.sent_from" is a dup. Always null. Old sent_from has been replaced by "payload.call_rating_location"
    "payload.call_rating_location" AS sent_from,
    "payload.uploaded_debug_logs" AS uploaded_debug_logs,
    NULL AS imputed_timestamp,
    "payload.call_direction" AS call_direction,
    "payload.call_duration" AS call_duration,
    "payload.last_network" AS last_network,
    inserted_timestamp
FROM {{ source('party_planner_realtime', 'call_ratings') }}

UNION ALL SELECT
    created_at::DATE AS date_utc,
    created_at::DATE AS created_date,
    client_type,
    NULL AS android_id,
    NULL AS adjust_id,
    NULL AS firebase_id,
    NULL AS google_analytics_unique_id,
    NULL AS battery_level,
    NULL AS bluetooth_mac_address,
    NULL AS board_name,
    NULL AS bootloader_version,
    NULL AS brand_name,
    NULL AS build_fingerprint,
    NULL AS build_id,
    NULL AS device_id,
    NULL AS device_name,
    NULL AS google_play_services_advertising_id,
    NULL AS hardware_name,
    NULL AS iccid,
    NULL AS is_user_a_monkey,
    NULL AS manufacturer_name,
    NULL AS model_name,
    NULL AS os_base,
    NULL AS os_codename,
    NULL AS os_version,
    NULL AS product_name,
    NULL AS radio_version,
    NULL AS screen_height,
    NULL AS screen_width,
    NULL AS sdk_version,
    NULL AS uptime,
    NULL AS wifi_mac_address,
    NULL AS brand,
    NULL AS is_gifted_device,
    NULL AS stripe_token,
    NULL AS client_ip_address,
    NULL AS client_platform,
    NULL AS client_port,
    NULL AS client_version,
    NULL AS country_code,
    NULL AS language_code,
    NULL AS client_calling_sdk_version,
    NULL AS tz_code,
    user_agent,
    NULL AS date_of_birth,
    NULL AS email,
    NULL AS email_status,
    NULL AS first_name,
    NULL AS last_name,
    username,
    NULL AS idfa,
    NULL AS idfv,
    NULL AS create_time_server_offset,
    created_at,
    NULL AS event_id,
    NULL AS instance_id,
    NULL AS proto_class,
    NULL AS user_id,
    NULL AS user_id_hex,
    platform,
    call_id,
    call_rating,
    call_problem,
    sent_from,
    uploaded_debug_logs,
    imputed_timestamp,
    NULL AS call_direction,
    NULL AS call_duration,
    NULL AS last_network,
    inserted_timestamp
FROM {{ source('apphealth', 'call_ratings')}}
