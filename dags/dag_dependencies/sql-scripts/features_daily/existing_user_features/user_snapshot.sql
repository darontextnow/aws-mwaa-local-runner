CREATE OR REPLACE TRANSIENT TABLE analytics_staging.existing_user_features_user_snapshot AS
SELECT username, date_utc, user_id_hex, email, voicemail
FROM (
    SELECT username, date_utc
    FROM analytics.user_daily_activities
    WHERE
        (date_utc = '{{ ds }}'::DATE)
        AND (client_type IN ('TN_ANDROID', 'TN_IOS_FREE', '2L_ANDROID', 'TN_WEB'))
        AND (sms_messages + mms_messages + total_outgoing_calls > 0)
    UNION SELECT username, DATE(request_ts) date_utc
    FROM analytics.trust_safety_user_request_logs
    WHERE
        (request_ts >= '{{ data_interval_start }}'::TIMESTAMP_NTZ)
        AND (request_ts < '{{ data_interval_start }}'::TIMESTAMP_NTZ + INTERVAL '1 DAY')
        AND (route_name IN ('SessionsController_login', 'UsersController_register', 'v3IdentityAuthenticate'))
        AND (http_response_status = 200)
)
LEFT JOIN core.users USING (username)
