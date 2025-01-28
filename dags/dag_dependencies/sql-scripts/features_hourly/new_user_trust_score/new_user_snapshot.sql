MERGE INTO core.new_user_snaphot AS tgt USING (
    SELECT
        a.username,
        a.event_timestamp,
        a.client_type,
        b.user_id_hex,
        c.pp_user_id_hex,
        CURRENT_TIMESTAMP load_time,
        '{{ data_interval_start }}'::TIMESTAMP execution_time,
        a.device_fingerprint,
        a.ip_address,
        a.gps_adid,
        a.android_id
    FROM (
        SELECT username, event_timestamp, client_type, "bonus.Fingerprint" device_fingerprint, ip_address, gps_adid, android_id
        FROM core.sketchy_registration_logs
        WHERE
            (event_timestamp BETWEEN '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOUR'
                AND '{{ data_interval_start }}'::TIMESTAMP_NTZ)
            AND (action = 'allow_request')
            AND (username <> '')
        QUALIFY ROW_NUMBER() OVER(PARTITION BY username ORDER BY event_timestamp DESC) = 1
    ) a
    LEFT JOIN (
        SELECT username, user_id_hex
        FROM core.users
        WHERE (created_at > '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOUR')
    ) b ON (a.username = b.username)
    LEFT JOIN (
        SELECT DISTINCT "client_details.client_data.user_data.username" username, user_id_hex pp_user_id_hex
        FROM party_planner_realtime.registration
        WHERE
            (instance_id LIKE 'TN_SERVER%')
            AND (created_at > '{{ data_interval_start }}'::TIMESTAMP_NTZ - INTERVAL '1 HOUR')
            AND ("payload.result" = 'RESULT_OK')
    ) c ON (a.username = c.username)
) AS src ON
    (tgt.execution_time = src.execution_time)
    AND (tgt.username = src.username)
    AND (NVL(tgt.pp_user_id_hex, 'x') = NVL(src.pp_user_id_hex, 'x'))
WHEN MATCHED THEN UPDATE SET
    tgt.username = src.username,
    tgt.event_timestamp = src.event_timestamp,
    tgt.client_type = src.client_type,
    tgt.user_id_hex = src.user_id_hex,
    tgt.pp_user_id_hex = src.pp_user_id_hex,
    tgt.load_time = src.load_time,
    tgt.execution_time = src.execution_time,
    tgt.device_fingerprint = src.device_fingerprint,
    tgt.ip_address = src.ip_address,
    tgt.gps_adid = src.gps_adid,
    tgt.android_id = src.android_id
WHEN NOT MATCHED THEN INSERT VALUES (
    src.username,
    src.event_timestamp,
    src.client_type,
    src.user_id_hex,
    src.pp_user_id_hex,
    src.load_time,
    src.execution_time,
    src.device_fingerprint,
    src.ip_address,
    src.gps_adid,
    src.android_id
)