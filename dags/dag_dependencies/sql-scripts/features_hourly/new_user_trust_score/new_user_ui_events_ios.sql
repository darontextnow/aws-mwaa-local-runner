INSERT INTO core.new_user_ui_events_ios
WITH ios_device_ids AS (
    SELECT username, event_timestamp, b.idfv, c.adid
    FROM core.new_user_snaphot a
    LEFT JOIN (
        SELECT * FROM firehose.registrations
        WHERE
            (created_at > '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 HOUR')
            AND (http_response_status = 200)
    ) b USING (username)
    LEFT JOIN core.users USING (username)
    LEFT JOIN (
        SELECT * FROM adjust.registrations WHERE (created_at > '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 HOUR')
    ) c USING (username)
    WHERE
        (client_type = 'TN_IOS_FREE')
        AND (NVL(account_status, 'ENABLED') = 'ENABLED')
        AND (execution_time = '{{ data_interval_start }}'::TIMESTAMP)
)
SELECT
    username,
    created_at,
    '{{ data_interval_start }}'::TIMESTAMP,
    "client_details.ios_bonus_data.adjust_id",
    "client_details.ios_bonus_data.carrier_country",
    "client_details.ios_bonus_data.carrier_iso_country_code",
    "client_details.ios_bonus_data.carrier_mobile_country_code",
    "client_details.ios_bonus_data.carrier_mobile_network_code",
    "client_details.ios_bonus_data.cfuuid",
    "client_details.ios_bonus_data.google_analytics_unique_id",
    "client_details.ios_bonus_data.idfa",
    "client_details.ios_bonus_data.idfv",
    "client_details.ios_bonus_data.model_name",
    "client_details.ios_bonus_data.os_version",
    "client_details.ios_bonus_data.product_name",
    "client_details.ios_bonus_data.screen_height",
    "client_details.ios_bonus_data.screen_width",
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
FROM party_planner_realtime.propertymap a
JOIN ios_device_ids ON ("client_details.ios_bonus_data.idfv" = idfv)
WHERE
    (date_utc BETWEEN DATE('{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 HOUR') AND DATE('{{ data_interval_start }}'::TIMESTAMP + INTERVAL '1 HOUR'))
    AND (COALESCE("payload.properties.UITracking_Category", "payload.properties.UITracking_Action",
        "payload.properties.UITracking_Label", "payload.properties.UITracking_Value") IS NOT NULL)
    AND ("client_details.client_data.client_platform" = 'IOS')
    AND (created_at BETWEEN event_timestamp - INTERVAL '120 SECONDS' AND event_timestamp + INTERVAL '1 HOUR')
ORDER BY 1, 2;
