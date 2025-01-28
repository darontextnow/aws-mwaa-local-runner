CREATE OR REPLACE TRANSIENT TABLE analytics_staging.all_known_user_ips_90days AS
WITH req_ips AS (
    SELECT username, reg_ip, client_ip ip_address, request_ts event_time
    FROM analytics.trust_safety_user_request_logs
    JOIN analytics_staging.existing_user_features_active_users_metadata USING (username)
    WHERE
        (request_ts BETWEEN '{{ ds }}'::TIMESTAMP_NTZ - INTERVAL '90 DAYS' AND '{{ ds }}'::TIMESTAMP_NTZ)
        AND (ip_address NOT LIKE '10.%')
),
req_ips_recent AS (
    SELECT username,reg_ip,client_ip ip_address,request_ts event_time
    FROM core.tn_requests_raw
    JOIN analytics_staging.existing_user_features_active_users_metadata USING (username)
    WHERE
        (request_ts BETWEEN '{{ ds }}'::TIMESTAMP_NTZ - INTERVAL '3 DAYS' AND '{{ ds }}'::TIMESTAMP_NTZ)
        AND (ip_address NOT LIKE '10.%')
        AND (route_name NOT LIKE 'Adjust%')
        AND (client_type IS NOT NULL)
        AND (route_name != 'SessionsController_getSessionUser')
),
msg_ips AS (
    SELECT username,reg_ip,"payload.origin_metadata.ip" ip_address,created_at event_time
    FROM party_planner_realtime.messagedelivered
    JOIN analytics_staging.existing_user_features_active_users_metadata USING (user_id_hex)
    WHERE
        (created_at BETWEEN '{{ ds }}'::TIMESTAMP_NTZ - INTERVAL '90 DAYS' AND '{{ ds }}'::TIMESTAMP_NTZ)
        AND (instance_id LIKE 'MESS%')
        AND (LEN(ip_address) > 0)
        AND (ip_address NOT LIKE '10.%')
        AND ("payload.message_direction" LIKE '%OUTBOUND')
)
SELECT * FROM req_ips
UNION ALL SELECT * FROM msg_ips
UNION ALL SELECT * FROM req_ips_recent
