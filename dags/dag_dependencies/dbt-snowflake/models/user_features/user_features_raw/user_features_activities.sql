{{
    config(
        tags=['daily_features'],
        full_refresh = false,
        materialized='incremental',
        unique_key='date_utc'
    )
}}

SELECT
    date_utc,
    username,
    SUM(NVL(ad_impressions, 0)) AS ad_impressions,
    SUM(NVL(MMS_messages, 0)) AS MMS_messages,
    SUM(NVL(SMS_messages, 0)) AS SMS_messages,
    SUM(NVL(total_outgoing_calls, 0)) AS total_outgoing_calls,
    SUM(NVL(total_outgoing_call_duration, 0)) AS total_outgoing_call_duration,
    SUM(NVL(total_outgoing_unique_calling_contacts, 0)) AS total_outgoing_unique_calling_contacts,
    SUM(NVL(total_outgoing_free_calls, 0)) AS total_outgoing_free_calls,
    SUM(NVL(total_outgoing_paid_calls, 0)) AS total_outgoing_paid_calls,
    SUM(NVL(total_outgoing_domestic_calls, 0)) AS total_outgoing_domestic_calls,
    SUM(NVL(total_outgoing_international_calls, 0)) AS total_outgoing_international_calls,
    SUM(NVL(total_incoming_calls, 0)) AS total_incoming_calls,
    SUM(NVL(total_incoming_call_duration, 0)) AS total_incoming_call_duration,
    SUM(NVL(total_incoming_unique_calling_contacts, 0)) AS total_incoming_unique_calling_contacts,
    SUM(NVL(total_incoming_free_calls, 0)) AS total_incoming_free_calls,
    SUM(NVL(total_incoming_paid_calls, 0)) AS total_incoming_paid_calls,
    SUM(NVL(total_incoming_domestic_calls, 0)) AS total_incoming_domestic_calls,
    SUM(NVL(total_incoming_international_calls, 0)) AS total_incoming_international_calls,
    SUM(NVL(video_call_initiations, 0)) AS video_call_initiations
FROM {{ ref('user_daily_activities') }}
WHERE (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('ds') }}::DATE)
GROUP BY 1, 2
