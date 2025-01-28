{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_SMALL'
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
    MAX("payload.data.INCOMING_CALL_V2_METRICS.application_version") AS application_version,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.call_accept_time") AS call_accept_time,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.call_disposition") AS call_disposition,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.call_establish_time") AS call_establish_time,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.call_id") AS call_id,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.call_id_or_global_call_id") AS call_id_or_global_call_id,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.call_type") AS call_type,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.client_type") AS client_type,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.create_time") AS create_time,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.device_model") AS device_model,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.global_call_id") AS global_call_id,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.intercept_call_establish_time") AS intercept_call_establish_time,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.intercept_call_start_time") AS intercept_call_start_time,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.intercept_call_trying_time") AS intercept_call_trying_time,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.intercept_sdp_ready_time") AS intercept_sdp_ready_time,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.intercept_uuid") AS intercept_uuid,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.network_tester_result") AS network_tester_result,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.operating_system") AS operating_system,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.os_version") AS os_version,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.platform_host") AS platform_host,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.push_receive_time") AS push_receive_time,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.sip_client") AS sip_client,
    CURRENT_TIMESTAMP AS inserted_at,
    'dbt' AS inserted_by,
    MAX("payload.data.INCOMING_CALL_V2_METRICS.created_at_wall_time") AS created_at_wall_time    
FROM  {{ source('party_planner_realtime', 'callgenericevent') }}
WHERE 
    (("payload.data.INCOMING_CALL_V2_METRICS.application_version" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.call_accept_time" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.call_disposition" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.call_establish_time" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.call_id" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.call_id_or_global_call_id" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.call_type" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.client_type" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.create_time" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.device_model" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.global_call_id" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.intercept_call_establish_time" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.intercept_call_start_time" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.intercept_call_trying_time" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.intercept_sdp_ready_time" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.intercept_uuid" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.network_tester_result" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.operating_system" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.os_version" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.platform_host" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.push_receive_time" IS NOT NULL)
    OR ("payload.data.INCOMING_CALL_V2_METRICS.sip_client" IS NOT NULL))

{% if is_incremental() %}
    AND (date_utc >= (SELECT MAX(date_utc) - INTERVAL '1 day' FROM {{ this }} ))
{% endif %}

GROUP BY 1, 2, 3
