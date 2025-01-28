WITH messages_master AS (
    SELECT
        p.user_id_hex,
        CASE
            WHEN p."payload.message_direction" = 'MESSAGE_DIRECTION_OUTBOUND' AND m.value::TEXT LIKE 'tel:+%' THEN SPLIT_PART(m.value::TEXT, ':', 2)
            WHEN p."payload.message_direction" = 'MESSAGE_DIRECTION_OUTBOUND' AND REGEXP_LIKE(m.value::TEXT, 'tel:1[0-9]{10}') THEN '+' || SPLIT_PART(m.value::TEXT, ':', 2)
            WHEN p."payload.message_direction" = 'MESSAGE_DIRECTION_OUTBOUND' AND m.value::TEXT LIKE 'tel:%' THEN '+1' || SPLIT_PART(m.value::TEXT, ':', 2)
            WHEN p."payload.message_direction" = 'MESSAGE_DIRECTION_INBOUND' AND p."payload.origin"[0]::TEXT LIKE 'tel:+%' THEN SPLIT_PART(p."payload.origin"[0]::TEXT, ':', 2)
            WHEN p."payload.message_direction" = 'MESSAGE_DIRECTION_INBOUND' AND REGEXP_LIKE(p."payload.origin"[0]::TEXT, 'tel:1[0-9]{10}') THEN '+' || SPLIT_PART(p."payload.origin"[0]::TEXT, ':', 2)
            WHEN p."payload.message_direction" = 'MESSAGE_DIRECTION_INBOUND' AND p."payload.origin"[0]::TEXT LIKE 'tel:%' THEN '+1' || SPLIT_PART(p."payload.origin"[0]::TEXT, ':', 2)
            ELSE NULL
        END AS normalized_contact
    FROM
        party_planner_realtime.messagedelivered p
        , LATERAL FLATTEN("payload.target") m
    WHERE
        (p.created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 hours' AND '{{ data_interval_start }}'::TIMESTAMP)
        AND (p."payload.content_type" IN ('MESSAGE_TYPE_TEXT', 'MESSAGE_TYPE_IMAGE', 'MESSAGE_TYPE_VIDEO', 'MESSAGE_TYPE_AUDIO'))
        AND (p.instance_id LIKE 'MESS%')
    GROUP BY 1, 2
),

stage_data AS (
    SELECT
        username,
        ARRAY_AGG(DISTINCT(normalized_contact)) AS contacts
    FROM (
        SELECT
            username,
            OBJECT_CONSTRUCT('S', m.normalized_contact) AS normalized_contact
        FROM messages_master m
        LEFT JOIN core.users c ON c.user_id_hex = m.user_id_hex
        WHERE normalized_contact IS NOT NULL

        UNION ALL SELECT
            "client_details.client_data.user_data.username" AS username,
            CASE
                WHEN "payload.call_direction" = 'CALL_DIRECTION_OUTBOUND' THEN OBJECT_CONSTRUCT('S', "payload.destination")
                WHEN "payload.call_direction" = 'CALL_DIRECTION_INBOUND' THEN OBJECT_CONSTRUCT('S', "payload.destination")
                ELSE NULL
            END AS normalized_contact
        FROM party_planner_realtime.callcompleted
        WHERE
            (created_at BETWEEN '{{ data_interval_start }}'::TIMESTAMP - INTERVAL '1 hours' AND '{{ data_interval_start }}'::TIMESTAMP)
            AND (normalized_contact IS NOT NULL)
    ) AS s
    GROUP BY 1
)

SELECT
    OBJECT_CONSTRUCT(
        'user_key', OBJECT_CONSTRUCT('S', username),
        'hour_window', OBJECT_CONSTRUCT('S', '{{ data_interval_start }}'::TIMESTAMP_NTZ),
        'contacts', OBJECT_CONSTRUCT('L', contacts)
    ) AS json_data
FROM stage_data
WHERE username IS NOT NULL
