MERGE INTO core.user_contact_graph USING (
    WITH tmp_new_contact_records AS (
        SELECT a.*, b.latest_contact_day AS prev_latest
        FROM analytics_staging.tmp_user_contact_graph_pp_call_mess_contacts_last_4_days a
        LEFT JOIN core.user_contact_graph b ON (a.user_id_hex = b.user_id_hex) AND (a.normalized_contact = b.normalized_contact)
        WHERE
            (a.user_id_hex != '000-00-000-000000000')
            AND ((contact_day > latest_contact_day) OR (latest_contact_day IS NULL))
    )
    SELECT
        user_id_hex,
        normalized_contact,
        MIN(CASE WHEN dir = 'CALL_DIRECTION_INBOUND' THEN contact_day ELSE NULL END) inbound_call_earliest_day,
        MIN(CASE WHEN dir = 'CALL_DIRECTION_OUTBOUND' THEN contact_day ELSE NULL END) outbound_call_earliest_day,
        MIN(CASE WHEN dir = 'MESSAGE_DIRECTION_INBOUND' THEN contact_day ELSE NULL END) inbound_mess_earliest_day,
        MIN(CASE WHEN dir = 'MESSAGE_DIRECTION_OUTBOUND' THEN contact_day ELSE NULL END) outbound_mess_earliest_day,
        MIN(contact_day) earliest_contact_day,
        MAX(CASE WHEN dir = 'CALL_DIRECTION_INBOUND' THEN contact_day ELSE NULL END) inbound_call_latest_day,
        MAX(CASE WHEN dir = 'CALL_DIRECTION_OUTBOUND' THEN contact_day ELSE NULL END) outbound_call_latest_day,
        MAX(CASE WHEN dir = 'MESSAGE_DIRECTION_INBOUND' THEN contact_day ELSE NULL END) inbound_mess_latest_day,
        MAX(CASE WHEN dir = 'MESSAGE_DIRECTION_OUTBOUND' THEN contact_day ELSE NULL END) outbound_mess_latest_day,
        MAX(contact_day) latest_contact_day,
        COUNT(DISTINCT contact_day) num_days_with_interaction,
        SUM(CASE WHEN dir = 'CALL_DIRECTION_INBOUND' THEN interaction_qty ELSE 0 END) inbound_call_duration_seconds,
        SUM(CASE WHEN dir = 'CALL_DIRECTION_OUTBOUND' THEN interaction_qty ELSE 0 END) outbound_call_duration_seconds,
        SUM(CASE WHEN dir = 'MESSAGE_DIRECTION_INBOUND' THEN interaction_qty ELSE 0 END) inbound_messages,
        SUM(CASE WHEN dir = 'MESSAGE_DIRECTION_OUTBOUND' THEN interaction_qty ELSE 0 END) outbound_messages
    FROM tmp_new_contact_records
    GROUP BY 1, 2
) merge_records ON
    (user_contact_graph.user_id_hex = merge_records.user_id_hex)
    AND (user_contact_graph.normalized_contact=merge_records.normalized_contact)
WHEN MATCHED AND (merge_records.latest_contact_day > user_contact_graph.latest_contact_day)
THEN UPDATE SET
    user_contact_graph.latest_contact_day=merge_records.latest_contact_day,
    user_contact_graph.inbound_call_earliest_day = COALESCE(user_contact_graph.inbound_call_earliest_day,merge_records.inbound_call_earliest_day),
    user_contact_graph.outbound_call_earliest_day = COALESCE(user_contact_graph.outbound_call_earliest_day,merge_records.outbound_call_earliest_day),
    user_contact_graph.inbound_mess_earliest_day = COALESCE(user_contact_graph.inbound_mess_earliest_day,merge_records.inbound_mess_earliest_day),
    user_contact_graph.outbound_mess_earliest_day = COALESCE(user_contact_graph.outbound_mess_earliest_day,merge_records.outbound_mess_earliest_day),
    user_contact_graph.inbound_call_latest_day = COALESCE(merge_records.inbound_call_latest_day,user_contact_graph.inbound_call_latest_day),
    user_contact_graph.outbound_call_latest_day = COALESCE(merge_records.outbound_call_latest_day,user_contact_graph.outbound_call_latest_day),
    user_contact_graph.inbound_mess_latest_day = COALESCE(merge_records.inbound_mess_latest_day,user_contact_graph.inbound_mess_latest_day),
    user_contact_graph.outbound_mess_latest_day = COALESCE(merge_records.outbound_mess_latest_day,user_contact_graph.outbound_mess_latest_day),
    user_contact_graph.num_days_with_interaction = user_contact_graph.num_days_with_interaction + merge_records.num_days_with_interaction,
    user_contact_graph.inbound_call_duration_seconds = user_contact_graph.inbound_call_duration_seconds + merge_records.inbound_call_duration_seconds,
    user_contact_graph.outbound_call_duration_seconds = user_contact_graph.outbound_call_duration_seconds + merge_records.outbound_call_duration_seconds,
    user_contact_graph.inbound_messages = user_contact_graph.inbound_messages + merge_records.inbound_messages,
    user_contact_graph.outbound_messages = user_contact_graph.outbound_messages + merge_records.outbound_messages
WHEN NOT MATCHED THEN INSERT (
    user_id_hex,
    normalized_contact,
    inbound_call_earliest_day,
    outbound_call_earliest_day,
    inbound_mess_earliest_day,
    outbound_mess_earliest_day,
    earliest_contact_day,
    inbound_call_latest_day,
    outbound_call_latest_day,
    inbound_mess_latest_day,
    outbound_mess_latest_day,
    latest_contact_day,
    num_days_with_interaction,
    inbound_call_duration_seconds,
    outbound_call_duration_seconds,
    inbound_messages,
    outbound_messages 
)
VALUES (
    merge_records.user_id_hex,
    merge_records.normalized_contact,
    merge_records.inbound_call_earliest_day,
    merge_records.outbound_call_earliest_day,
    merge_records.inbound_mess_earliest_day,
    merge_records.outbound_mess_earliest_day,
    merge_records.earliest_contact_day,
    merge_records.inbound_call_latest_day,
    merge_records.outbound_call_latest_day,
    merge_records.inbound_mess_latest_day,
    merge_records.outbound_mess_latest_day,
    merge_records.latest_contact_day,
    merge_records.num_days_with_interaction,
    merge_records.inbound_call_duration_seconds,
    merge_records.outbound_call_duration_seconds,
    merge_records.inbound_messages,
    merge_records.outbound_messages
);
