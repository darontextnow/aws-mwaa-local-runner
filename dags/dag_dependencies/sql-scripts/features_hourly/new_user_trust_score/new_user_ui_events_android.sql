INSERT INTO core.new_user_ui_events_android
WITH android_device_ids AS (
    SELECT
        username,
        event_timestamp,
        gps_adid,
        a.android_id,
        c.adid,
        CASE WHEN adid IS NOT NULL THEN 'adjust_id'
             WHEN REGEXP_LIKE(gps_adid,'^[0-]+$') = FALSE THEN 'gps_adid'
             ELSE  'android_id'
        END AS device_id_used
    FROM core.new_user_snaphot a
    LEFT JOIN core.users b USING (username)
    LEFT JOIN (
        SELECT DISTINCT username, adid FROM adjust.registrations
        WHERE created_at > '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 HOUR'
    ) c
    USING (username)
    WHERE
        (client_type IN ('TN_ANDROID','2L_ANDROID'))
        AND (NVL(account_status, 'ENABLED') ='ENABLED')
        AND (execution_time = '{{ data_interval_start }}'::TIMESTAMP)
)
SELECT
    username,
    device_id_used,
    created_at,
    '{{ data_interval_start }}'::TIMESTAMP,
    "client_details.android_bonus_data.adjust_id",
    "client_details.android_bonus_data.android_id",
    "client_details.android_bonus_data.battery_level",
    "client_details.android_bonus_data.bluetooth_mac_address",
    "client_details.android_bonus_data.board_name",
    "client_details.android_bonus_data.bootloader_version",
    "client_details.android_bonus_data.brand_name",
    "client_details.android_bonus_data.build_fingerprint",
    "client_details.android_bonus_data.build_id",
    "client_details.android_bonus_data.device_id",
    "client_details.android_bonus_data.device_name",
    "client_details.android_bonus_data.firebase_id",
    "client_details.android_bonus_data.google_play_services_advertising_id",
    "client_details.android_bonus_data.google_analytics_unique_id",
    "client_details.android_bonus_data.hardware_name",
    "client_details.android_bonus_data.iccid",
    "client_details.android_bonus_data.manufacturer_name",
    "client_details.android_bonus_data.model_name",
    "client_details.android_bonus_data.os_base",
    "client_details.android_bonus_data.os_codename",
    "client_details.android_bonus_data.os_version",
    "client_details.android_bonus_data.product_name",
    "client_details.android_bonus_data.radio_version",
    "client_details.android_bonus_data.radio_height",
    "client_details.android_bonus_data.screen_width",
    "client_details.android_bonus_data.sdk_version",
    "client_details.android_bonus_data.uptime",
    "client_details.android_bonus_data.wifi_mac_address",
    "client_details.client_data.client_ip_address",
    "client_details.client_data.client_platform",
    "client_details.client_data.client_version",
    "client_details.client_data.language_code",
    "client_details.client_data.tz_code",
    "client_details.client_data.user_data.email",
    "client_details.client_data.user_data.email_status",
    "client_details.client_data.user_data.username",
    instance_id,
    "payload.type",
    "payload.properties"
FROM (
    SELECT username, device_id_used, a.*
    FROM party_planner_realtime.propertymap a
    JOIN android_device_ids ON ("client_details.android_bonus_data.adjust_id" = adid)
    WHERE
        (date_utc BETWEEN DATE('{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 HOUR') AND DATE('{{ data_interval_start }}'::TIMESTAMP + INTERVAL '1 HOUR'))
        AND (device_id_used = 'adjust_id')
        AND (COALESCE("payload.properties.UITracking_Category", "payload.properties.UITracking_Action",
            "payload.properties.UITracking_Label", "payload.properties.UITracking_Value") IS NOT NULL)
        AND ("client_details.client_data.client_platform"='CP_ANDROID')
        AND (created_at BETWEEN event_timestamp - INTERVAL '120 SECONDS' AND event_timestamp + INTERVAL '1 HOUR')

    UNION ALL SELECT username, device_id_used, a.*
    FROM party_planner_realtime.propertymap a
    JOIN android_device_ids ON ("client_details.android_bonus_data.google_play_services_advertising_id" = gps_adid)
    WHERE
        (date_utc BETWEEN DATE('{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 HOUR') AND DATE('{{ data_interval_start }}'::TIMESTAMP + INTERVAL '1 HOUR'))
        AND (device_id_used='gps_adid')
        AND (COALESCE("payload.properties.UITracking_Category", "payload.properties.UITracking_Action",
            "payload.properties.UITracking_Label", "payload.properties.UITracking_Value") IS NOT NULL)
        AND ("client_details.client_data.client_platform"='CP_ANDROID')
        AND (created_at BETWEEN event_timestamp - INTERVAL '120 SECONDS' AND event_timestamp + INTERVAL '1 HOUR')

    UNION ALL SELECT username, device_id_used, a.*
    FROM party_planner_realtime.propertymap a
    JOIN android_device_ids ON ("client_details.android_bonus_data.android_id" = android_id)
    WHERE
        (date_utc BETWEEN DATE('{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 HOUR') AND DATE('{{ data_interval_start }}'::TIMESTAMP + INTERVAL '1 HOUR'))
        AND (device_id_used='android_id')
        AND (COALESCE("payload.properties.UITracking_Category", "payload.properties.UITracking_Action",
            "payload.properties.UITracking_Label", "payload.properties.UITracking_Value") IS NOT NULL)
        AND ("client_details.client_data.client_platform"='CP_ANDROID')
        AND (created_at BETWEEN event_timestamp - INTERVAL '120 SECONDS' AND event_timestamp + INTERVAL '1 HOUR')
);
