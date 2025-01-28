{{
    config(
        tags=['daily_features'],
        full_refresh = false,
        materialized='incremental',
        unique_key='date_utc'
    )
}}

SELECT
    created_at::DATE AS date_utc,
    username,
    MAX(score) AS nps_max_score,
    MIN(score) AS nps_min_score,
    MAX(CASE WHEN final_action = 'submit' THEN score END) AS nps_submitted_score,
    LISTAGG(final_action, ',') AS nps_final_action
FROM {{ ref('nps') }}
WHERE (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('ds') }}::DATE)
GROUP BY 1, 2
