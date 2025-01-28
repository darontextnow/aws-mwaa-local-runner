{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH user_client_type AS (
    SELECT
        user_id_hex,
        client_type,
        user_set_id
    FROM {{ source('dau', 'user_device_master') }} d
    JOIN {{ ref('analytics_users') }} u
        ON (d.username = u.username)
        AND (date_utc < {{ var('ds') }}::DATE + INTERVAL '1 day')
        AND (user_set_id IS NOT NULL)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY u.user_id_hex ORDER BY d.created_at DESC) = 1
),
pp_userlevel_messages AS (
    SELECT
        created_at::DATE AS date_utc,
        user_id_hex,
        "client_details.client_data.user_data.username" AS username,
        "payload.origin_locale.country_code" AS country_code,
        "payload.message_direction",
        "payload.gateway"
    FROM {{ source('party_planner_realtime', 'message_delivered') }},
    --Flattened the table to include group MMS; otherwise, it would be treated as a single count.
    LATERAL FLATTEN(input => "payload.target") target
    WHERE
    {% if is_incremental() or target.name == 'dev' %}
        (created_at::DATE BETWEEN {{ var('ds') }}::DATE - INTERVAL '1 week' AND {{ var('ds') }}::DATE)
    {% endif %}
        --Filter to avoid duplicate messages
        AND ("payload.routing_decision" = 'ROUTING_DECISION_ALLOW')
        AND ("payload.gateway" NOT IN ('GATEWAY_UNKNOWN'))
        AND user_id_hex IS NOT NULL
)
SELECT
    m.date_utc,
    c.user_set_id,
    m.user_id_hex,
    username,
    c.client_type,
    m.country_code,
    SUM(CASE WHEN "payload.message_direction" = 'MESSAGE_DIRECTION_OUTBOUND' THEN 1 ELSE 0 END)
        AS total_outbound_messages,
    SUM(CASE WHEN "payload.message_direction" = 'MESSAGE_DIRECTION_INBOUND' THEN 1 ELSE 0 END)
        AS total_inbound_messages
FROM pp_userlevel_messages m
JOIN user_client_type c ON (c.user_id_hex = m.user_id_hex)
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1 DESC