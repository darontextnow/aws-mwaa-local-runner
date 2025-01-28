{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

{% if is_incremental() %}
  {% set query %}
      SELECT MAX(date_utc) - INTERVAL '1 day' FROM {{ this }}
  {% endset %}

  {% if execute %}
      {% set max_dt = run_query(query).columns[0][0] %}
  {% else %}
      {% set max_dt = '2018-03-01' %}
  {% endif %}
{% endif %}

SELECT 
    date_utc
    ,event_id
    ,ANY_VALUE(instance_id) AS instance_id
    ,ANY_VALUE(created_at) AS created_at
    ,ANY_VALUE(user_id_hex) AS user_id_hex
    ,ANY_VALUE(user_id) AS user_id
    ,ANY_VALUE("payload.leg_a_call_id") AS leg_a_call_id   
    ,ANY_VALUE("client_details.client_data.user_data.username") AS username   
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.call_id") AS call_id     
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.call_started_at") AS call_started_at      
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.call_ended_at") AS call_ended_at     
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.client_type") AS client_type
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.call_direction") AS call_direction      
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.packets_received") AS packets_received
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.packets_sent") AS packets_sent   
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.packet_loss") AS packet_loss
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.min_jitter") AS min_jitter 
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.max_jitter") AS max_jitter
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.computed_mos") AS computed_mos
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.max_call_volume") AS max_call_volume
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.application_name") AS application_name  
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.application_version") AS application_version      
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.platform") AS platform    
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.call_disposition") AS call_disposition
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.sip_client") AS sip_client
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.registrar_domain") AS registrar_domain
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.end_call_volume") AS end_call_volume
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.client_config_group") AS client_config_group
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.qos_test_network") AS qos_test_network
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.rtp_setup_time") AS rtp_setup_time
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.initial_call_volume") AS initial_call_volume
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.ice_server_type") AS ice_server_type
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.qos_test_chosen_network") AS qos_test_chosen_network
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.qos_test_result") AS qos_test_result
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.call_duration") AS call_duration  
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.used_turn") AS used_turn
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.initial_network") AS initial_network
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.num_network_switches") AS num_network_switches
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.codec") AS codec
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.opus_bitrate") AS opus_bitrate
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.num_consecutive_bad_mos") AS num_consecutive_bad_mos
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.bad_mos_started_at") AS bad_mos_started_at
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.num_bad_mos_periods") AS num_bad_mos_periods
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.total_bad_mos_samples_within_periods") AS total_bad_mos_samples_within_periods
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.last_mos_samples") AS last_mos_samples
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.fs_config_group") AS fs_config_group
    ,ANY_VALUE("client_details.client_data.client_ip_address") AS client_ip_address
    ,ANY_VALUE("payload.data.LEGACY_END_CALL_METRICS.isConference") AS is_conference
FROM {{ source('party_planner_realtime', 'callgenericevent') }}
WHERE
    ("payload.data.LEGACY_END_CALL_METRICS.call_ended_at" IS NOT NULL)
 
{% if is_incremental() %}
    AND (date_utc >= '{{ max_dt }}'::DATE)
{% endif %}

{% if target.name == 'dev' %}
    AND (date_utc::DATE >= CURRENT_DATE - INTERVAL '1 week')
{% endif %}

GROUP BY 1, 2