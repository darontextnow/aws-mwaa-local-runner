-- IP addresses for registrations (minus internal traffic etc) and outbound messages
-- This tmp table is now shared by both android and ios user_ips_info_tmp tables
CREATE TRANSIENT TABLE user_ips_tmp_{{ ts_nodash }} AS
WITH req_ips AS (
    SELECT username, client_ip ip_address, request_ts event_time, client_type
    FROM core.new_user_request_logs
    JOIN user_cohort_tmp_{{ ts_nodash }} USING (username)
    WHERE
        (request_ts BETWEEN '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '30 DAYS'
            AND '{{ data_interval_start }}'::TIMESTAMP + INTERVAL '1 HOUR')
        AND (ip_address NOT LIKE '10.%')
        AND (route_name NOT LIKE 'Adjust%')
        AND (route_name NOT ILIKE '%disable%')
        AND (client_type IN ('TN_ANDROID','2L_ANDROID','TN_IOS_FREE','TN_WEB'))
        AND (route_name != 'SessionsController_getSessionUser')
),

msg_ips AS (
    --using client_type = 'any' here as there is no client_type. Will include all these rows in downstream filters
    SELECT username, "payload.origin_metadata.ip" ip_address, created_at event_time, 'any' AS client_type
    FROM party_planner_realtime.messagedelivered
    JOIN user_cohort_tmp_{{ ts_nodash }} USING (user_id_hex)
    WHERE
        (inserted_timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 HOUR'
            AND '{{ data_interval_start }}'::TIMESTAMP + INTERVAL '1 HOUR')
        AND (instance_id LIKE 'MESS%')
        AND (LEN(ip_address) > 0)
        AND (ip_address NOT LIKE '10.%')
        AND ("payload.message_direction" LIKE '%OUTBOUND')
)

SELECT * FROM req_ips
UNION ALL SELECT * FROM msg_ips;
