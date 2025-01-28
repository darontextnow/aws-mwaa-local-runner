CREATE OR REPLACE TRANSIENT TABLE analytics_staging.tmp_user_contact_graph_pp_call_mess_contacts_last_4_days AS
WITH tmp_pp_mess_last_4days AS (
    SELECT
        user_id_hex,
        DATE(created_at) msg_day,
        "payload.message_direction",
        "payload.target",
        "payload.origin",
        COUNT(*) num_msg
    FROM party_planner_realtime.messagedelivered
    WHERE
        (DATE(created_at) BETWEEN '{{ ds }}'::DATE - INTERVAL '4 DAYS' AND '{{ ds }}'::DATE)
        AND (instance_id LIKE 'MESS%')
    GROUP BY 1, 2, 3, 4, 5
),
messages_flattened AS (
    SELECT
        user_id_hex,
        msg_day ,
        "payload.message_direction" dir,
        "payload.origin"[0]::text origin,
        num_msg ,
        m.value::text dest
    FROM tmp_pp_mess_last_4days
    ,LATERAL FLATTEN("payload.target") m
),
tmp_pp_calls_last_4days AS (
    SELECT
        user_id_hex,
        DATE(created_at) call_day,
        "payload.call_direction",
        "payload.origin",
        "payload.destination",
        SUM("payload.call_duration.seconds") call_dur,
        MAX("payload.call_duration.seconds") max_call_dur,
        COUNT(*) num_calls
    FROM party_planner_realtime.callcompleted
    WHERE (DATE(created_at) BETWEEN '{{ ds }}'::DATE - INTERVAL '4 DAYS' AND '{{ ds }}'::DATE)
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    user_id_hex,
    call_day contact_day,
    "payload.call_direction" dir,
    CASE WHEN dir='CALL_DIRECTION_INBOUND' THEN "payload.origin" ELSE "payload.destination" END normalized_contact,
    call_dur interaction_qty
FROM tmp_pp_calls_last_4days
WHERE (LEN(normalized_contact) > 0)
UNION ALL SELECT
    user_id_hex,
    msg_day contact_day,
    dir,
    CASE
        WHEN dir='MESSAGE_DIRECTION_OUTBOUND' AND dest LIKE 'tel:+%' THEN split_part(dest,':',2)
        WHEN dir='MESSAGE_DIRECTION_OUTBOUND' AND dest LIKE 'tel:%' THEN '+1' || split_part(dest,':',2)
        WHEN dir='MESSAGE_DIRECTION_INBOUND' AND origin LIKE 'tel:+%' THEN split_part(origin,':',2)
        WHEN dir='MESSAGE_DIRECTION_INBOUND' AND origin LIKE 'tel:%' THEN '+1' || split_part(origin,':',2)
    ELSE NULL END AS normalized_contact,num_msg interaction_qty
FROM messages_flattened
WHERE (normalized_contact IS NOT NULL) AND (LEN(normalized_contact) > 0)