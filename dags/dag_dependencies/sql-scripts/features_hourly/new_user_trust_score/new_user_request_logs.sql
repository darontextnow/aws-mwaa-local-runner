INSERT INTO core.new_user_request_logs
WITH sketchy_logs AS (
    SELECT username, event_timestamp
    FROM core.sketchy_registration_logs
    WHERE
        (event_timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOUR'
            AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
        AND (action = 'allow_request')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY username ORDER BY event_timestamp DESC) = 1
)

SELECT 
    insertion_time,
    request_ts,
    tn_requests_raw.username,
    client_type,
    client_version,
    client_ip,
    route_name,
    http_response_status,
    "X-TN-Integrity-Session",
    authentication_type,
    perimeterx,
    error_code,
    type,
    client_id
FROM core.tn_requests_raw
JOIN sketchy_logs ON (tn_requests_raw.username = sketchy_logs.username)
WHERE
    (request_ts BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOUR'
        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ + INTERVAL '1 HOUR')
    AND (request_ts < event_timestamp + INTERVAL '1 HOUR')
    AND (insertion_time < event_timestamp + INTERVAL '1 HOUR')

UNION ALL SELECT
    insertion_time,
    request_ts,
    tn_requests2_raw.username,
    client_type,
    client_version,
    client_ip,
    route_name,
    http_response_status,
    "X-TN-Integrity-Session",
    authentication_type,
    perimeterx,
    error_code,
    type,
    client_id
FROM core.tn_requests2_raw
JOIN sketchy_logs ON (tn_requests2_raw.username = sketchy_logs.username)
WHERE
    (request_ts BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOUR'
        AND '{{ data_interval_start }}'::TIMESTAMP_NTZ + INTERVAL '1 HOUR')
    AND (request_ts < event_timestamp + INTERVAL '1 HOUR')
    AND (insertion_time < event_timestamp + INTERVAL '1 HOUR')
;
