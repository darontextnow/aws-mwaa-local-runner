{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'

    )
}}

WITH filtered_sessions AS (
    SELECT
        created_at::DATE AS date_utc,
        "client_details.client_data.user_data.username" AS username,
        {{ pp_normalized_client_type() }} AS client_type,
        CASE
            WHEN "payload.time_in_session" >= 1800 * 1000 THEN 1800 * 1000 ---more than 30min is capped to 30min
            ELSE "payload.time_in_session"
        END AS time_in_session_capped

    FROM {{ source('party_planner_realtime', 'app_lifecycle_changed') }}
    WHERE
    {% if is_incremental() or target.name == 'dev' %}
        (created_at::DATE BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 days' AND {{ var('ds') }}::DATE)
    {% else %}
        (created_at::DATE BETWEEN '2024-07-01' AND CURRENT_DATE)
    {% endif %}
        AND ("client_details.client_data.country_code" IN ('US', 'CA'))
        AND "payload.time_in_session" > 0 --condition for true sessions
        AND ("client_details.client_data.client_platform" IN ('CP_ANDROID', 'IOS'))
        AND LENGTH("client_details.client_data.user_data.username") > 0 --capture only non-empty strings

)
SELECT
    s.date_utc,
    s.username,
    client_type,
    COUNT(*) AS num_sessions,
    --converted to mins from PP's incoming milliseconds sessions
    SUM(time_in_session_capped / (1000 * 60.0)) AS time_in_app_mins_per_day,
    SUM(time_in_session_capped / (1000 * 3600.0)) AS time_in_app_hours_per_day,
    CURRENT_TIMESTAMP AS inserted_timestamp
FROM filtered_sessions s
GROUP BY 1, 2, 3
