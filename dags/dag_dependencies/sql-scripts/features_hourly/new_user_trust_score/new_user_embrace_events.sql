INSERT INTO core.new_user_embrace_events
SELECT 
    username,
    app_id,
    app_version,
    build_id,
    country,
    device_id,
    environment,
    manufacturer,
    model,
    msg_id,
    name,
    os_version,
    received_ts,
    session_id,
    sdk_version,
    ts,
    user_id,
    LEFT(value, 25600),
    details,
    '{{ execution_date }}'::TIMESTAMP
FROM (
    SELECT * FROM core.embrace_data 
    WHERE (received_ts BETWEEN DATE_PART(EPOCH_SECOND, '{{ execution_date }}'::TIMESTAMP - INTERVAL '1 HOUR')
        AND DATE_PART(EPOCH_SECOND, '{{ execution_date }}'::TIMESTAMP + INTERVAL '1 HOUR'))
) a
JOIN (
    SELECT DISTINCT username,user_id_hex, event_timestamp
    FROM core.new_user_snaphot
    WHERE (execution_time = '{{ execution_date }}'::TIMESTAMP)
) b ON (b.user_id_hex = a.user_id) AND (received_ts < DATE_PART(EPOCH_SECOND, event_timestamp + INTERVAL '1 HOUR'))
JOIN core.users USING (username)
WHERE (account_status = 'ENABLED') AND ( name NOT LIKE 'anr-%');
