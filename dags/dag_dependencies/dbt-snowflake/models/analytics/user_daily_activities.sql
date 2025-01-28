{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}

WITH revenue_user_daily_ad AS (
    SELECT date_utc,
           username,
           {{ normalized_client_type('client_type') }} As client_type,
           SUM(NVL(adjusted_impressions, 0)) AS ad_impressions,
           SUM(NVL(adjusted_revenue, 0)) AS total_adjusted_revenue
    FROM {{ ref ('revenue_user_daily_ad') }}
    WHERE (username IS NOT NULL)

    {% if is_incremental() or target.name == 'dev' %}

        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 WEEK')

    {% endif %}

    GROUP BY 1, 2, 3
),
cost_user_daily_message_cost AS (
    SELECT date_utc,
           username,
           {{ normalized_client_type('client_type') }} AS client_type,
           SUM(NVL(MMS_number_messages, 0)) AS MMS_messages,
           SUM(NVL(SMS_number_messages, 0)) AS SMS_messages,
           SUM(NVL(total_mms_cost, 0)) AS total_mms_cost,
           SUM(NVL(total_sms_cost, 0)) AS total_sms_cost
    FROM {{ ref ('cost_user_daily_message_cost')}}
    WHERE (username IS NOT NULL)

    {% if is_incremental() or target.name == 'dev' %}

        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 WEEK')

    {% endif %}

    GROUP BY 1, 2, 3
),
cost_user_daily_call_cost AS (
    SELECT date_utc,
           username,
           {{ normalized_client_type('client_type') }} AS client_type,
           SUM(NVL(termination_cost, 0)) AS total_termination_cost,
           SUM(NVL(layered_paas_cost, 0)) AS total_layered_paas_cost
    FROM {{ ref ('cost_user_daily_call_cost')}}
    WHERE (username IS NOT NULL)

    {% if is_incremental() or target.name == 'dev' %}

        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 WEEK')

    {% endif %}

    GROUP BY 1, 2, 3
),
user_outgoing_calling_activity AS (
    SELECT date_utc,
           username,
           {{ normalized_client_type('client_type') }} AS client_type,
           NVL(COUNT(created_at), 0) AS total_outgoing_calls,
           SUM(NVL(call_secs, 0)) AS total_outgoing_call_duration,
           NVL(COUNT(distinct outgoing_call_contact), 0) As total_outgoing_unique_calling_contacts,
           COUNT(CASE WHEN is_free THEN 1 END) AS total_outgoing_free_calls,
           NVL(COUNT(created_at), 0) - COUNT(CASE WHEN is_free THEN 1 END) AS total_outgoing_paid_calls,
           COUNT(CASE WHEN is_domestic THEN 1 END) AS total_outgoing_domestic_calls,
           NVL(COUNT(created_at), 0) - COUNT(CASE WHEN is_domestic THEN 1 END) AS total_outgoing_international_calls
    FROM {{ ref ('outgoing_calls')}}
    WHERE (username IS NOT NULL)

    {% if is_incremental() or target.name == 'dev' %}

        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 WEEK')

    {% endif %}

    GROUP BY 1, 2, 3
),
user_incoming_calling_activity AS (
    SELECT date_utc,
           username,
           {{ normalized_client_type('client_type') }} AS client_type,
           NVL(COUNT(created_at), 0) AS total_incoming_calls,
           SUM(NVL(call_secs, 0)) AS total_incoming_call_duration,
           NVL(COUNT(distinct incoming_call_contact), 0) As total_incoming_unique_calling_contacts,
           COUNT(CASE WHEN is_free THEN 1 END) AS total_incoming_free_calls,
           NVL(COUNT(created_at), 0) - COUNT(CASE WHEN is_free THEN 1 END) AS total_incoming_paid_calls,
           COUNT(CASE WHEN is_domestic THEN 1 END) AS total_incoming_domestic_calls,
           NVL(COUNT(created_at), 0) - COUNT(CASE WHEN is_domestic THEN 1 END) AS total_incoming_international_calls
    FROM {{ ref ('incoming_calls') }}
    WHERE (username IS NOT NULL)

    {% if is_incremental() or target.name == 'dev' %}

        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 WEEK')

    {% endif %}

    GROUP BY 1, 2, 3
),
user_video_call_activity AS (
    SELECT
        created_at::DATE AS date_utc,
        "client_details.client_data.user_data.username" AS username,
        {{ pp_normalized_client_type() }} AS client_type,
        COUNT(1) AS video_call_initiations
    FROM {{ source('party_planner_realtime', 'video_call_initiate') }}
    WHERE "payload.success"

    {% if is_incremental() or target.name == 'dev' %}

        AND (created_at >= {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '1 WEEK')

    {% endif %}

    GROUP BY 1, 2, 3
)
SELECT date_utc,
       username,
       client_type,
       NVL(ad_impressions, 0) AS ad_impressions,
       NVL(total_adjusted_revenue, 0) AS total_adjusted_revenue,
       NVL(MMS_messages, 0) AS MMS_messages,
       NVL(SMS_messages, 0) AS SMS_messages,
       NVL(total_mms_cost, 0) AS total_mms_cost,
       NVL(total_sms_cost, 0) AS total_sms_cost,
       NVL(total_termination_cost, 0) AS total_termination_cost,
       NVL(total_layered_paas_cost, 0) AS total_layered_paas_cost,
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
FROM revenue_user_daily_ad
FULL OUTER JOIN cost_user_daily_message_cost USING (date_utc, username, client_type)
FULL OUTER JOIN cost_user_daily_call_cost USING (date_utc, username, client_type)
FULL OUTER JOIN user_outgoing_calling_activity USING (date_utc, username, client_type)
FULL OUTER JOIN user_incoming_calling_activity USING (date_utc, username, client_type)
FULL OUTER JOIN user_video_call_activity USING (date_utc, username, client_type)
WHERE (username IS NOT NULL)
