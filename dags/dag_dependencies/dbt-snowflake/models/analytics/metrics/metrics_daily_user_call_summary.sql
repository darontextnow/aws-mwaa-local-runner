{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH pp_userlevel_calls AS (
    SELECT
        created_at::DATE AS date_utc,
        user_id_hex,
        "client_details.client_data.user_data.username" AS username,
        "payload.call_direction" AS call_direction,
        "payload.call_duration.seconds" AS duration,
        "payload.origin_locale.country_code" AS country_code,
        {{ pp_normalized_client_type() }} AS client_type
    FROM {{ source('party_planner_realtime', 'call_completed') }}
    WHERE
    {% if is_incremental() or target.name == 'dev' %}
        (created_at::DATE BETWEEN {{ var('ds') }}::DATE - INTERVAL '1 week' AND {{ var('ds') }}::DATE)
    {% endif %}
        --Filter out incoming missed calls
        AND NOT ("payload.call_direction" = 'CALL_DIRECTION_INBOUND' AND "payload.call_duration.seconds" = 0)
        AND user_id_hex IS NOT NULL
)
SELECT
    a.date_utc,
    users.user_set_id,
    a.user_id_hex,
    a.username,
    a.client_type,
    a.country_code,
    SUM(CASE WHEN call_direction = 'CALL_DIRECTION_OUTBOUND' THEN 1 ELSE 0 END) AS total_outbound_calls,
    SUM(CASE WHEN call_direction = 'CALL_DIRECTION_INBOUND' THEN 1 ELSE 0 END) AS total_inbound_calls,
    SUM(CASE WHEN call_direction = 'CALL_DIRECTION_OUTBOUND' THEN duration ELSE 0 END) AS total_outbound_call_duration,
    SUM(CASE WHEN call_direction = 'CALL_DIRECTION_INBOUND' THEN duration ELSE 0 END) AS total_inbound_call_duration
FROM pp_userlevel_calls a
JOIN {{ ref('analytics_users') }} users ON (a.user_id_hex = users.user_id_hex)
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1 DESC