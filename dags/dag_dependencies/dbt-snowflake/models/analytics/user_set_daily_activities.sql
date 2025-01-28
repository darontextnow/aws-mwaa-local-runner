{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}
WITH dau AS (
    SELECT
        date_utc,
        user_set_id,
        client_type,
        dau
    FROM {{ ref('dau_user_set_active_days') }}
    WHERE

    {% if is_incremental() %}
        (date_utc >= {{ var('ds') }}::DATE - INTERVAL '7 DAYS')
    {% endif %}

        AND (user_set_id IS NOT NULL)
),
daily_activity_table AS (
    SELECT
        date_utc,
        COALESCE(set_uuid, username) AS user_set_id,
        client_type,
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
    FROM {{ ref('user_daily_activities') }} daily_activity
    LEFT JOIN {{ source('dau', 'user_set') }} userset USING (username)
    WHERE

    {% if is_incremental() %}
        (date_utc >= {{ var('ds') }}::DATE - INTERVAL '7 DAYS')
    {% endif %}

        AND (username IS NOT NULL)
    GROUP BY 1, 2, 3
)
SELECT date_utc,
       user_set_id,
       client_type,
       NVL(dau, CASE WHEN client_type = 'TN_WEB' THEN 1 ELSE 0 END) AS dau,
       NVL(ad_impressions, 0) AS ad_impressions,
       NVL(MMS_messages, 0) AS MMS_messages,
       NVL(SMS_messages, 0) AS SMS_messages,
       NVL(total_outgoing_calls, 0) AS total_outgoing_calls,
       NVL(total_outgoing_call_duration, 0) AS total_outgoing_call_duration,
       NVL(total_outgoing_unique_calling_contacts, 0) AS total_outgoing_unique_calling_contacts,
       NVL(total_outgoing_free_calls, 0) AS total_outgoing_free_calls,
       NVL(total_outgoing_paid_calls, 0) AS total_outgoing_paid_calls,
       NVL(total_outgoing_domestic_calls, 0) AS total_outgoing_domestic_calls,
       NVL(total_outgoing_international_calls, 0) AS total_outgoing_international_calls,
       NVL(total_incoming_calls, 0) AS total_incoming_calls,
       NVL(total_incoming_call_duration, 0) AS total_incoming_call_duration,
       NVL(total_incoming_unique_calling_contacts, 0) AS total_incoming_unique_calling_contacts,
       NVL(total_incoming_free_calls, 0) AS total_incoming_free_calls,
       NVL(total_incoming_paid_calls, 0) AS total_incoming_paid_calls,
       NVL(total_incoming_domestic_calls, 0) AS total_incoming_domestic_calls,
       NVL(total_incoming_international_calls, 0) AS total_incoming_international_calls,
       NVL(video_call_initiations, 0) AS video_call_initiations
FROM dau
FULL OUTER JOIN daily_activity_table USING (date_utc, user_set_id, client_type)
WHERE user_set_id IS NOT NULL
