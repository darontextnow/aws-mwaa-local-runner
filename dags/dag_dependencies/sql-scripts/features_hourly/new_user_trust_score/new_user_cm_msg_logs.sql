INSERT INTO core.new_user_cm_msg_logs SELECT
    username,
    event_time,
    account_id,
    action,
    detail,
    msg,
    direction,
    reason,
    "TO",
    verdict,
    client_ip,
    '{{ execution_date }}'::TIMESTAMP execution_time
FROM core.cm_msg_spam_logs a
JOIN (
    SELECT DISTINCT username,user_id_hex, event_timestamp
    FROM core.new_user_snaphot
    WHERE (execution_time = '{{ execution_date }}'::TIMESTAMP)
) b ON (account_id = user_id_hex)
WHERE
    (a.event_time > '{{ execution_date }}'::TIMESTAMP - INTERVAL '1 HOUR')
    AND (a.event_time < b.event_timestamp + INTERVAL '1 HOUR')
;
