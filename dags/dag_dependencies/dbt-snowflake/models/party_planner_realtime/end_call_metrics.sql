{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}


SELECT
    date_utc,
    created_at,
    event_id,
    MAX(instance_id) AS instance_id,
    MAX(user_id_hex) AS user_id_hex,
    MAX(user_id) AS user_id,
    MAX("payload.leg_a_call_id") AS leg_a_call_id,
    MAX("client_details.client_data.user_data.username") AS username,
    MAX("client_details.client_data.client_port") AS client_port,
    MAX("client_details.client_data.language_code") AS language_code,
    MAX("client_details.client_data.country_code") AS country_code,
    MAX("client_details.client_data.checkout_workflow_data") AS checkout_workflow_data,
    MAX("client_details.client_data.client_calling_sdk_version") AS client_calling_sdk_version,
    MAX("client_details.client_data.client_ip_address") AS client_ip_address,
    MAX("client_details.client_data.client_platform") AS client_platform,
    MAX("client_details.client_data.user_data.date_of_birth") AS date_of_birth,
    MAX("client_details.client_data.user_data.last_name") AS last_name,
    MAX("client_details.client_data.user_data.first_name") AS first_name,
    MAX("client_details.client_data.user_data.email_status") AS email_status,
    MAX("client_details.client_data.user_data.email") AS email,
    MAX("client_details.client_data.client_version") AS client_version,
    MAX("client_details.client_data.tz_code") AS tz_code,
    MAX("client_details.client_data.brand") AS brand,
    MAX("client_details.client_data.user_agent") AS user_agent,
    MAX("payload.data.END_CALL_METRICS.audio_codec_all_calls") AS audio_codec_all_calls,
    MAX("payload.data.END_CALL_METRICS.average_audio_energy_received") AS average_audio_energy_received,
    MAX("payload.data.END_CALL_METRICS.average_audio_energy_sent") AS average_audio_energy_sent,
    MAX("payload.data.END_CALL_METRICS.average_jitter_buffer_delay_ms") AS average_jitter_buffer_delay_ms,
    MAX("payload.data.END_CALL_METRICS.average_latency_ms") AS average_latency_ms,
    MAX("payload.data.END_CALL_METRICS.average_mos") AS average_mos,
    MAX("payload.data.END_CALL_METRICS.fec_packets_discarded") AS fec_packets_discarded,
    MAX("payload.data.END_CALL_METRICS.fec_packets_received") AS fec_packets_received,
    MAX("payload.data.END_CALL_METRICS.ice_hosts") AS ice_hosts,
    MAX("payload.data.END_CALL_METRICS.latency_jump_count") AS latency_jump_count,
    MAX("payload.data.END_CALL_METRICS.max_audio_jitter_buffer_packets") AS max_audio_jitter_buffer_packets,
    MAX("payload.data.END_CALL_METRICS.max_latency_ms") AS max_latency_ms,
    MAX("payload.data.END_CALL_METRICS.network_tester_reconnect_max_wait_time") AS network_tester_reconnect_max_wait_time,
    MAX("payload.data.END_CALL_METRICS.no_inbound_audio_timeout") AS no_inbound_audio_timeout,
    MAX("payload.data.END_CALL_METRICS.no_outbound_audio_timeout") AS no_outbound_audio_timeout,
    MAX("payload.data.END_CALL_METRICS.packet_loss_percentage") AS packet_loss_percentage,
    MAX("payload.data.END_CALL_METRICS.sip_call_id") AS sip_call_id,
    MAX("payload.data.END_CALL_METRICS.total_audio_energy_received") AS total_audio_energy_received,
    MAX("payload.data.END_CALL_METRICS.total_audio_energy_sent") AS total_audio_energy_sent,
    MAX("payload.data.END_CALL_METRICS.total_samples_duration_received") AS total_samples_duration_received,
    MAX("payload.data.END_CALL_METRICS.total_samples_duration_sent") AS total_samples_duration_sent,
    MAX("payload.data.END_CALL_METRICS.turn_url") AS turn_url,
    CURRENT_TIMESTAMP AS inserted_timestamp
FROM  {{ source('party_planner_realtime', 'callgenericevent') }}
WHERE 
    (("payload.data.END_CALL_METRICS.audio_codec_all_calls" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.average_audio_energy_received" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.average_audio_energy_sent" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.average_jitter_buffer_delay_ms" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.average_latency_ms" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.average_mos" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.fec_packets_discarded" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.fec_packets_received" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.ice_hosts" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.latency_jump_count" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.max_audio_jitter_buffer_packets" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.max_latency_ms" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.network_tester_reconnect_max_wait_time" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.no_inbound_audio_timeout" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.no_outbound_audio_timeout" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.packet_loss_percentage" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.sip_call_id" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.total_audio_energy_received" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.total_audio_energy_sent" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.total_samples_duration_received" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.total_samples_duration_sent" IS NOT NULL)
    OR ("payload.data.END_CALL_METRICS.turn_url" IS NOT NULL))

{% if is_incremental() %}
    AND (date_utc >= (SELECT MAX(date_utc) - INTERVAL '1 day' FROM {{ this }} ))
{% endif %}

    GROUP BY 1, 2, 3
