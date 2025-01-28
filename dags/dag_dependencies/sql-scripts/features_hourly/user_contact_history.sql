WITH messages_data AS (
    SELECT
        p.user_id_hex,
        CASE
            WHEN p."payload.content_type" = 'MESSAGE_TYPE_TEXT' THEN 'SMS'
            WHEN p."payload.content_type" IN ('MESSAGE_TYPE_IMAGE', 'MESSAGE_TYPE_VIDEO', 'MESSAGE_TYPE_AUDIO') THEN 'MMS'
            ELSE 'other'
        END AS type,
        p."payload.message_direction" AS dir,
        CASE
            WHEN dir = 'MESSAGE_DIRECTION_OUTBOUND' AND m.value::VARCHAR LIKE 'tel:+%' THEN split_part(m.value::VARCHAR,':',2)
            WHEN dir = 'MESSAGE_DIRECTION_OUTBOUND' AND REGEXP_LIKE(m.value::VARCHAR,'tel:1[0-9]{10}') THEN '+' || split_part(m.value::VARCHAR,':',2)
            WHEN dir = 'MESSAGE_DIRECTION_OUTBOUND' AND m.value::VARCHAR LIKE 'tel:%' THEN '+1' || split_part(m.value::VARCHAR,':',2)
            WHEN dir = 'MESSAGE_DIRECTION_INBOUND' AND p."payload.origin"[0]::VARCHAR LIKE 'tel:+%' THEN split_part(p."payload.origin"[0]::VARCHAR,':',2)
            WHEN dir = 'MESSAGE_DIRECTION_INBOUND' AND REGEXP_LIKE(p."payload.origin"[0]::VARCHAR, 'tel:1[0-9]{10}') THEN '+' || split_part(p."payload.origin"[0]::VARCHAR,':',2)
            WHEN dir = 'MESSAGE_DIRECTION_INBOUND' AND p."payload.origin"[0]::VARCHAR LIKE 'tel:%' THEN '+1' || split_part(p."payload.origin"[0]::VARCHAR,':',2)
            ELSE NULL
        END AS normalized_contact,
        CASE WHEN ARRAY_SIZE(p."payload.target") > 1 THEN 1 ELSE 0 END AS group_flag,
        COUNT(*) AS num_msgs
    FROM
        party_planner_realtime.messagedelivered p,
        LATERAL FLATTEN("payload.target") m
    WHERE
        (p.created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 hours' AND '{{ data_interval_start }}'::TIMESTAMP)
        AND (p.instance_id LIKE 'MESS%')
        AND (type IN ('SMS', 'MMS'))
    GROUP BY 1, 2, 3, 4, 5
),

user_contact_msgs AS (
    SELECT
        c.username || '_' || m.normalized_contact AS user_key,
        '{{ data_interval_start }}'::TIMESTAMP_NTZ AS hour_window,
        SUM(CASE WHEN m.dir = 'MESSAGE_DIRECTION_OUTBOUND' AND m.group_flag = 0 AND m.type = 'SMS' THEN m.num_msgs ELSE 0 END) AS outgoing_normal_text_messages,
        SUM(CASE WHEN m.dir = 'MESSAGE_DIRECTION_OUTBOUND' AND m.group_flag = 1 AND m.type = 'SMS' THEN m.num_msgs ELSE 0 END) AS outgoing_group_messages,
        SUM(CASE WHEN m.dir = 'MESSAGE_DIRECTION_OUTBOUND' AND m.group_flag = 0 AND m.type = 'MMS' THEN m.num_msgs ELSE 0 END) AS outgoing_normal_mms_messages,
        SUM(CASE WHEN m.dir = 'MESSAGE_DIRECTION_OUTBOUND' AND m.group_flag = 1 AND m.type = 'MMS' THEN m.num_msgs ELSE 0 END) AS outgoing_group_mms_messages,
        SUM(CASE WHEN m.dir = 'MESSAGE_DIRECTION_INBOUND' AND m.group_flag = 0 AND m.type = 'SMS' THEN m.num_msgs ELSE 0 END) AS incoming_normal_text_messages,
        SUM(CASE WHEN m.dir = 'MESSAGE_DIRECTION_INBOUND' AND m.group_flag = 1 AND m.type = 'SMS' THEN m.num_msgs ELSE 0 END) AS incoming_group_messages,
        SUM(CASE WHEN m.dir = 'MESSAGE_DIRECTION_INBOUND' AND m.group_flag = 0 AND m.type = 'MMS' THEN m.num_msgs ELSE 0 END) AS incoming_normal_mms_messages,
        SUM(CASE WHEN m.dir = 'MESSAGE_DIRECTION_INBOUND' AND m.group_flag = 1 AND m.type = 'MMS' THEN m.num_msgs ELSE 0 END) AS incoming_group_mms_messages
    FROM messages_data m
    INNER JOIN core.users c ON c.user_id_hex = m.user_id_hex
    GROUP BY 1, 2
),

calls_data AS (
    SELECT
        "client_details.client_data.user_data.username" AS username,
        "payload.call_direction" AS dir,
        CASE
            WHEN dir = 'CALL_DIRECTION_OUTBOUND' THEN "payload.destination"
            WHEN dir = 'CALL_DIRECTION_INBOUND' THEN "payload.origin"
            ELSE NULL
        END AS normalized_contact,
        SUM("payload.call_duration.seconds") AS call_dur,
        COUNT(*) AS num_calls
    FROM party_planner_realtime.callcompleted
    WHERE (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 hours' AND '{{ data_interval_start }}'::TIMESTAMP)
    GROUP BY 1, 2, 3
),

user_contact_calls AS (
    SELECT
        username || '_' || normalized_contact AS user_key,
        '{{ data_interval_start }}'::TIMESTAMP_NTZ AS hour_window,
        SUM(CASE WHEN dir = 'CALL_DIRECTION_OUTBOUND' THEN num_calls ELSE 0 END) AS outgoing_calls,
        SUM(CASE WHEN dir = 'CALL_DIRECTION_OUTBOUND' THEN call_dur ELSE 0 END) AS outgoing_call_duration,
        SUM(CASE WHEN dir = 'CALL_DIRECTION_INBOUND' THEN num_calls ELSE 0 END) AS incoming_calls,
        SUM(CASE WHEN dir = 'CALL_DIRECTION_INBOUND' THEN call_dur ELSE 0 END) AS incoming_call_duration
    FROM calls_data
    GROUP BY 1, 2
)

SELECT
    OBJECT_CONSTRUCT(
        'user_key', OBJECT_CONSTRUCT('S', COALESCE(m.user_key, c.user_key)),
        'hour_window', OBJECT_CONSTRUCT('S', COALESCE(m.hour_window, c.hour_window)),
        'outgoing_normal_text_messages', OBJECT_CONSTRUCT('N', CAST(m.outgoing_normal_text_messages AS VARCHAR)),
        'outgoing_group_messages', OBJECT_CONSTRUCT('N', CAST(m.outgoing_group_messages AS VARCHAR)),
        'outgoing_normal_mms_messages', OBJECT_CONSTRUCT('N', CAST(m.outgoing_normal_mms_messages AS VARCHAR)),
        'outgoing_group_mms_messages', OBJECT_CONSTRUCT('N', CAST(m.outgoing_group_mms_messages AS VARCHAR)),
        'incoming_normal_text_messages', OBJECT_CONSTRUCT('N', CAST(m.incoming_normal_text_messages AS VARCHAR)),
        'incoming_group_messages', OBJECT_CONSTRUCT('N', CAST(m.incoming_group_messages AS VARCHAR)),
        'incoming_normal_mms_messages', OBJECT_CONSTRUCT('N', CAST(m.incoming_normal_mms_messages AS VARCHAR)),
        'incoming_group_mms_messages', OBJECT_CONSTRUCT('N', CAST(m.incoming_group_mms_messages AS VARCHAR)),
        'outgoing_calls', OBJECT_CONSTRUCT('N', CAST(c.outgoing_calls AS VARCHAR)),
        'outgoing_call_duration', OBJECT_CONSTRUCT('N', CAST(c.outgoing_call_duration AS VARCHAR)),
        'incoming_calls', OBJECT_CONSTRUCT('N', CAST(c.incoming_calls AS VARCHAR)),
        'incoming_call_duration', OBJECT_CONSTRUCT('N', CAST(c.incoming_call_duration AS VARCHAR))
    ) AS json_data
FROM user_contact_msgs m
FULL OUTER JOIN user_contact_calls c ON (m.user_key = c.user_key) AND (m.hour_window = c.hour_window)
WHERE COALESCE(m.user_key, c.user_key) IS NOT NULL
