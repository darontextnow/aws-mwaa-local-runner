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
    SUM(CASE WHEN event IN ('ASSIGN', 'PORTIN') THEN 1 ELSE 0 END) AS phone_number_assigns,
    SUM(CASE WHEN event IN ('UNASSIGN', 'PORTOUT') THEN 1 ELSE 0 END) AS phone_number_unassigns
FROM {{ ref('phone_number_assignments') }}
WHERE (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '3 DAYS' AND {{ var('ds') }}::DATE)
GROUP BY 1, 2
