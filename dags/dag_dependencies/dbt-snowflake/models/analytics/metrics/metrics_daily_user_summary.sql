{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

SELECT
    COALESCE(c.date_utc, m.date_utc) AS date_utc,
    COALESCE(c.user_set_id, m.user_set_id) AS user_set_id,
    SUM(total_outbound_calls) AS total_outbound_calls,
    --inbounds have not null as requested from BA team per DS-2732
    SUM(CASE WHEN c.client_type IS NOT NULL THEN total_inbound_calls ELSE 0 END) AS total_inbound_calls,
    SUM(total_outbound_calls) + SUM(total_inbound_calls) AS total_calls,
    SUM(total_outbound_call_duration) AS total_outbound_call_duration,
    SUM(CASE WHEN c.client_type IS NOT NULL THEN total_inbound_call_duration ELSE 0 END) AS total_inbound_call_duration,
    SUM(total_outbound_messages) AS total_outbound_messages,
    SUM(CASE WHEN m.client_type IS NOT NULL THEN total_inbound_messages ELSE 0 END) AS total_inbound_messages,
    SUM(total_outbound_messages) + SUM(total_inbound_messages) AS total_msgs,
    (COALESCE(total_calls, 0) + COALESCE(total_msgs, 0)) AS total_activity
FROM
    {{ ref('metrics_daily_user_call_summary') }} c
    FULL OUTER JOIN {{ ref('metrics_daily_user_message_summary') }} m
    ON c.date_utc = m.date_utc
    AND c.user_set_id = m.user_set_id
WHERE
{% if is_incremental() %}
    COALESCE(c.date_utc, m.date_utc)  >= {{ var('ds') }}::DATE - INTERVAL '1 week'
{% endif %}
GROUP BY 1, 2
ORDER BY 1 DESC