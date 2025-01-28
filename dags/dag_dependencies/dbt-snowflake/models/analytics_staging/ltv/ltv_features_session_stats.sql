{{
    config(
        tags=['daily_features'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

SELECT
    a.client_type,
    a.adjust_id,
    a.installed_at,
    ANY_VALUE(a.installed_at)::DATE AS date_utc,
    SUM(CASE WHEN a.created_at < installed_at + INTERVAL '3 days' THEN zeroifnull(a.last_time_spent) / 60 ELSE 0 END) AS time_in_app_3d,
    SUM(CASE WHEN a.created_at < installed_at + INTERVAL '5 days' THEN zeroifnull(a.last_time_spent) / 60 ELSE 0 END) AS time_in_app_5d,
    SUM(CASE WHEN a.created_at < installed_at + INTERVAL '7 days' THEN zeroifnull(a.last_time_spent) / 60 ELSE 0 END) AS time_in_app_7d,
    SUM(CASE WHEN a.created_at < installed_at + INTERVAL '3 days' THEN 1 ELSE 0 END) AS num_prior_sessions_3d,
    SUM(CASE WHEN a.created_at < installed_at + INTERVAL '5 days' THEN 1 ELSE 0 END) AS num_prior_sessions_5d,
    SUM(CASE WHEN a.created_at < installed_at + INTERVAL '7 days' THEN 1 ELSE 0 END) AS num_prior_sessions_7d
FROM {{ ref('adjust_sessions') }} a
JOIN {{ ref('ltv_numbered_installs') }} b USING (client_type, adjust_id, installed_at)
WHERE
    (a.created_at BETWEEN b.installed_at AND b.installed_at + INTERVAL '7 DAYS')

{% if is_incremental() %}
    AND (b.installed_at >= {{ var('ds') }}::DATE - INTERVAL '5 DAYS')
    AND (a.created_at >= {{ var('ds') }}::DATE - INTERVAL '5 DAYS')
{% else %}
    AND (b.installed_at >= '2018-03-01') AND (b.installed_at::DATE < {{ var('current_date') }} - INTERVAL '7 DAYS')
    AND (a.created_at >= '2018-03-01') AND (a.created_at < {{ var('current_date') }})
{% endif %}

GROUP BY 1, 2, 3
