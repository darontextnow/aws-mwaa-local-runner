-- this is a temporary table needed by all three downstream queries of the ip_info_update_hourly DAG
CREATE OR REPLACE TRANSIENT TABLE analytics_staging.ip_install_reg_login_ips_temp_{{ ts_nodash }} AS
SELECT ip_address
FROM adjust.registrations
WHERE (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '2 HOURS'
    AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)

UNION SELECT DISTINCT "client_details.client_data.client_ip_address" AS ip_address
FROM prod.party_planner_realtime.applifecyclechanged
WHERE
    --filter first by ordered date column for fast partition pruning
    (date_utc BETWEEN '{{ ds }}'::DATE - INTERVAL '1 DAY' AND '{{ ds }}'::DATE)
    --now apply more specific filtering by time
    AND (inserted_timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '2 HOURS'
        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ) -- use inserted_timestamp instead of created_at to capture late records
    AND ("payload.time_in_session" > 0) -- capturing only active sessions ip addresses

UNION SELECT client_ip ip_address
FROM firehose.registrations
WHERE
    (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '2 HOURS'
        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
    AND (http_response_status = 200)

UNION SELECT ip_address
FROM adjust.installs_with_pi
WHERE (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '2 HOURS'
    AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)

UNION SELECT ip_address
FROM adjust.installs_with_pi a
JOIN (
    SELECT DISTINCT adid
    FROM adjust.registrations
    WHERE  (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '2 HOURS'
        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
) b ON (a.adid = b.adid)

UNION SELECT client_ip ip_address
FROM core.tn_requests_raw
WHERE
    (insertion_time BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '2 HOURS'
        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
    AND (route_name IN ('SessionsController_login', 'v3IdentityAuthenticate', 'MessagesController_send',
        'UsersController_get', 'SessionsController_update', 'v3SendAttach'))

UNION SELECT client_ip ip_address
FROM analytics.trust_safety_user_request_logs
WHERE (request_ts > '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '48 HOURS')

UNION SELECT DISTINCT submission_ip ip_address
FROM core.disable_appeal_forms
WHERE 
    (PARSE_IP(submission_ip,'INET'):"family" = 4) -- ipv4 only
    AND (submission_created_at > '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '48 HOURS')
