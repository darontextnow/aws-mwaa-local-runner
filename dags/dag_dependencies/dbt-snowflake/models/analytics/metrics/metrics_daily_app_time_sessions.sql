{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

SELECT
    s.date_utc,
    CASE WHEN UPPER(client_type) IN ('TN_ANDROID', '2L_ANDROID') THEN 'ANDROID (INCL 2L)'
        ELSE client_type END AS client_type,
    SUM(num_sessions) AS num_sessions, --num_sessions from the source table is calculated on userlevel
    SUM(time_in_app_mins_per_day) AS time_in_app_mins_per_day,
    SUM(time_in_app_hours_per_day) AS time_in_app_hours_per_day,
    CURRENT_TIMESTAMP AS inserted_timestamp
FROM {{ ref ('metrics_daily_userlevel_app_time_sessions') }} s
JOIN {{ source('dau', 'user_set') }} user_set ON (s.username = user_set.username)
LEFT JOIN {{ ref('bad_sets') }} bad_sets ON (bad_sets.set_uuid = user_set.set_uuid)
WHERE
    (bad_sets.set_uuid IS NULL)
{% if is_incremental() or target.name == 'dev' %}
    AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '7 days')
{% else %}
    AND (date_utc BETWEEN '2024-07-01' AND {{ var('ds') }}::DATE)
{% endif %}

GROUP by 1, 2
