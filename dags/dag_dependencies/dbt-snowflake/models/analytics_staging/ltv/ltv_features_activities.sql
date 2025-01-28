{{
    config(
        tags=['daily_features'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}

SELECT
    b.client_type,
    b.adjust_id,
    b.installed_at,
    b.installed_at::DATE AS date_utc,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '3 DAYS' THEN dau END) AS dau_3d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '5 DAYS' THEN dau END) AS dau_5d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '7 DAYS' THEN dau END) AS dau_7d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '3 DAYS' THEN ad_impressions END) AS ad_impressions_3d,  -- only available after 2020-01-01
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '3 DAYS' THEN sms_messages END) AS sms_messages_3d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '3 DAYS' THEN mms_messages END) AS mms_messages_3d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '3 DAYS' THEN total_outgoing_calls END) AS total_outgoing_calls_3d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '3 DAYS' THEN total_outgoing_call_duration END) AS total_outgoing_call_duration_3d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '3 DAYS' THEN total_outgoing_unique_calling_contacts END) AS total_outgoing_unique_calling_contacts_3d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '3 DAYS' THEN total_outgoing_free_calls END) AS total_outgoing_free_calls_3d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '3 DAYS' THEN total_incoming_calls END) AS total_incoming_calls_3d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '3 DAYS' THEN total_incoming_call_duration END) AS total_incoming_call_duration_3d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '3 DAYS' THEN total_incoming_unique_calling_contacts END) AS total_incoming_unique_calling_contacts_3d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '5 DAYS' THEN ad_impressions END) AS ad_impressions_5d,  -- only available after 2020-01-01
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '5 DAYS' THEN sms_messages END) AS sms_messages_5d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '5 DAYS' THEN mms_messages END) AS mms_messages_5d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '5 DAYS' THEN total_outgoing_calls END) AS total_outgoing_calls_5d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '5 DAYS' THEN total_outgoing_call_duration END) AS total_outgoing_call_duration_5d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '5 DAYS' THEN total_outgoing_unique_calling_contacts END) AS total_outgoing_unique_calling_contacts_5d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '5 DAYS' THEN total_outgoing_free_calls END) AS total_outgoing_free_calls_5d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '5 DAYS' THEN total_incoming_calls END) AS total_incoming_calls_5d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '5 DAYS' THEN total_incoming_call_duration END) AS total_incoming_call_duration_5d,
    SUM(CASE WHEN date_utc < installed_at::DATE + INTERVAL '5 DAYS' THEN total_incoming_unique_calling_contacts END) AS total_incoming_unique_calling_contacts_5d,
    SUM(ad_impressions) AS ad_impressions_7d,  -- only available after 2020-01-01
    SUM(sms_messages) AS sms_messages_7d,
    SUM(mms_messages) AS mms_messages_7d,
    SUM(total_outgoing_calls) AS total_outgoing_calls_7d,
    SUM(total_outgoing_call_duration) AS total_outgoing_call_duration_7d,
    SUM(total_outgoing_unique_calling_contacts) AS total_outgoing_unique_calling_contacts_7d,
    SUM(total_outgoing_free_calls) AS total_outgoing_free_calls_7d,
    SUM(total_incoming_calls) AS total_incoming_calls_7d,
    SUM(total_incoming_call_duration) AS total_incoming_call_duration_7d,
    SUM(total_incoming_unique_calling_contacts) AS total_incoming_unique_calling_contacts_7d
FROM {{ ref('growth_installs_dau_generation') }} b
LEFT JOIN {{ ref('user_set_daily_activities') }} a ON
    (a.user_set_id = b.user_set_id)
    AND (a.client_type = b.client_type)
    AND (a.date_utc >= b.installed_at::DATE)
    AND (a.date_utc < b.installed_at::DATE + INTERVAL '7 DAYS')

{% if is_incremental() %}
    AND (a.date_utc >= {{ var('ds') }}::DATE - INTERVAL '12 DAYS') -- gives this a 5 day lookback from previous run.
{% endif %}

WHERE
    (b.installed_at::DATE < {{ var('current_date') }} - INTERVAL '7 DAYS')

{% if is_incremental() %}
    AND (b.installed_at >= {{ var('ds') }}::DATE - INTERVAL '12 DAYS') -- gives this a 5 day lookback from previous run.
{% else %}
    AND (b.installed_at >= '2018-03-01')
{% endif %}

GROUP BY 1, 2, 3, 4
