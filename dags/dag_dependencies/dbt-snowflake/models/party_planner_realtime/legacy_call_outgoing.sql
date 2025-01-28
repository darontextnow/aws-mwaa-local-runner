{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

SELECT
    date_utc
    ,event_id
    ,MAX(instance_id) AS instance_id
    ,MAX(created_at) AS created_at
    ,MAX(user_id_hex) AS user_id_hex
    ,MAX(user_id) AS user_id
    ,MAX("payload.leg_a_call_id") AS leg_a_call_id
    ,MAX("client_details.client_data.user_data.username") AS USERNAME 
    ,MAX("payload.data.LEGACY_OUTGOING_CALL_METRICS.client_type") AS client_type
    ,MAX("payload.data.LEGACY_OUTGOING_CALL_METRICS.os_version") AS os_version
    ,MAX("payload.data.LEGACY_OUTGOING_CALL_METRICS.device_model") AS device_model
    ,MAX("payload.data.LEGACY_OUTGOING_CALL_METRICS.operating_system") AS operating_system
    ,MAX("payload.data.LEGACY_OUTGOING_CALL_METRICS.application_version") AS application_version
    ,MAX("payload.data.LEGACY_OUTGOING_CALL_METRICS.sip_client") AS sip_client
    ,MAX("payload.data.LEGACY_OUTGOING_CALL_METRICS.call_type") AS call_type
    ,MAX("payload.data.LEGACY_OUTGOING_CALL_METRICS.call_id") AS call_id
    ,MAX(DATE_PART(EPOCH_SECOND,TO_TIMESTAMP("payload.data.LEGACY_OUTGOING_CALL_METRICS.call_trying_at"))) AS call_trying_at
    ,MAX(DATE_PART(EPOCH_SECOND,TO_TIMESTAMP("payload.data.LEGACY_OUTGOING_CALL_METRICS.call_initiated_at"))) AS call_initiated_at
    ,MAX(DATE_PART(EPOCH_SECOND,TO_TIMESTAMP("payload.data.LEGACY_OUTGOING_CALL_METRICS.call_placed_at"))) AS call_placed_at
    ,MAX(DATE_PART(EPOCH_SECOND,TO_TIMESTAMP("payload.data.LEGACY_OUTGOING_CALL_METRICS.call_started_at"))) AS call_started_at
    ,MAX("payload.data.LEGACY_OUTGOING_CALL_METRICS.network_tester_result") AS network_tester_result
    ,MAX("payload.data.LEGACY_OUTGOING_CALL_METRICS.created_at_wall_time") AS created_at_wall_time
    ,MAX("payload.data.LEGACY_OUTGOING_CALL_METRICS.fs_config_group") AS fs_config_group
    ,MAX("payload.data.LEGACY_OUTGOING_CALL_METRICS.client_config_group") AS client_config_group
FROM {{ source('party_planner_realtime', 'callgenericevent') }}
WHERE
    (1 = 1)

{% if is_incremental() %}
    AND (date_utc >= (select max(date_utc) - interval '1 day' FROM {{ this }} ))
{% endif %}

{% if target.name == 'dev' %}
    AND (date_utc::date >= CURRENT_DATE - INTERVAL '1 week')
{% endif %}

GROUP BY 1, 2
