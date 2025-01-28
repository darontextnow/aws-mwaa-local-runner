CREATE TRANSIENT TABLE user_cohort_tmp_{{ ts_nodash }} AS
SELECT username, user_id_hex, execution_time, device_fingerprint, client_type
FROM core.new_user_snaphot
WHERE
    (execution_time = '{{ data_interval_start }}'::TIMESTAMP_NTZ)
    AND (client_type IN ('TN_ANDROID', '2L_ANDROID', 'TN_IOS_FREE'))
