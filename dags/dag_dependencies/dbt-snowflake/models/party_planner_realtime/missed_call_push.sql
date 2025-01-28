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
    MAX("payload.data.MISSED_CALL_PUSH.contact") AS contact,
    MAX("payload.data.MISSED_CALL_PUSH.contact_type") AS contact_type,
    MAX("payload.data.MISSED_CALL_PUSH.contact_value") AS contact_value,
    MAX("payload.data.MISSED_CALL_PUSH.delay") AS "delay",
    MAX("payload.data.MISSED_CALL_PUSH.id") AS id,
    MAX("payload.data.MISSED_CALL_PUSH.leg_a_call_id") AS leg_a_call_id,
    MAX("payload.data.MISSED_CALL_PUSH.message") AS "message",
    MAX("payload.data.MISSED_CALL_PUSH.outgoing") AS outgoing,
    MAX(TO_TIMESTAMP("payload.data.MISSED_CALL_PUSH.received_time"::VARCHAR)) AS received_time,
    MAX(TO_TIMESTAMP("payload.data.MISSED_CALL_PUSH.sent_time"::VARCHAR)) AS sent_time,
    MAX("payload.data.MISSED_CALL_PUSH.type") AS "type",
    MAX("payload.data.MISSED_CALL_PUSH.uuid") AS uuid,
    CURRENT_TIMESTAMP AS inserted_timestamp
FROM  {{ source('party_planner_realtime', 'callgenericevent') }}
WHERE 
    (("payload.data.MISSED_CALL_PUSH.contact" IS NOT NULL)
    OR ("payload.data.MISSED_CALL_PUSH.contact_type" IS NOT NULL)
    OR ("payload.data.MISSED_CALL_PUSH.contact_value" IS NOT NULL)
    OR ("payload.data.MISSED_CALL_PUSH.delay" IS NOT NULL)
    OR ("payload.data.MISSED_CALL_PUSH.id" IS NOT NULL)
    OR ("payload.data.MISSED_CALL_PUSH.leg_a_call_id" IS NOT NULL)
    OR ("payload.data.MISSED_CALL_PUSH.message" IS NOT NULL)
    OR ("payload.data.MISSED_CALL_PUSH.outgoing" IS NOT NULL)
    OR ("payload.data.MISSED_CALL_PUSH.received_time" IS NOT NULL)
    OR ("payload.data.MISSED_CALL_PUSH.sent_time" IS NOT NULL)
    OR ("payload.data.MISSED_CALL_PUSH.type" IS NOT NULL)
    OR ("payload.data.MISSED_CALL_PUSH.uuid" IS NOT NULL))

{% if is_incremental() %}
    AND (date_utc >= (SELECT MAX(date_utc) - INTERVAL '1 day' FROM {{ this }} ))
{% endif %}

GROUP BY 1, 2, 3
