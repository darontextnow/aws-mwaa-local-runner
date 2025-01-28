{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}


SELECT 
    date_utc
    , HOUR(TO_TIMESTAMP(api_timestamp::VARCHAR)) AS recorded_hour
    , client_type
    , device_model
    , app_version
    , os_version
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'crash_total' THEN value ELSE 0 END) crash_total
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'crashed_free_session_pct' THEN value ELSE 0 END) crashed_free_session_pct
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'crashed_session_pct' THEN value ELSE 0 END) crashed_session_pct
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'crashed_user_total' THEN value ELSE 0 END) crashed_user_total
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'daily_crash_free_session_by_device_rate' THEN value ELSE 0 END) daily_crash_free_session_by_device_rate
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'daily_crash_free_session_rate' THEN value ELSE 0 END) daily_crash_free_session_rate
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'daily_crashed_users' THEN value ELSE 0 END) daily_crashed_users
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'daily_crashes_total' THEN value ELSE 0 END) daily_crashes_total
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'daily_sessions_by_device_total' THEN value ELSE 0 END) daily_sessions_by_device_total
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'daily_sessions_total' THEN value ELSE 0 END) daily_sessions_total
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'daily_users' THEN value ELSE 0 END) daily_users
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'hourly_crash_free_session_by_device_rate' THEN value ELSE 0 END) hourly_crash_free_session_by_device_rate
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'hourly_crash_free_session_rate' THEN value ELSE 0 END) hourly_crash_free_session_rate
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'hourly_crashed_users' THEN value ELSE 0 END) hourly_crashed_users
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'hourly_crashes_total' THEN value ELSE 0 END) hourly_crashes_total
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'hourly_sessions_by_device_total' THEN value ELSE 0 END) hourly_sessions_by_device_total
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'hourly_sessions_total' THEN value ELSE 0 END) hourly_sessions_total
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'hourly_users' THEN value ELSE 0 END) hourly_users
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'sessions_by_device_model_total' THEN value ELSE 0 END) sessions_by_device_model_total
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'sessions_total' THEN value ELSE 0 END) sessions_total
    , SUM(CASE WHEN LOWER(METRIC_NAME) = 'user_total' THEN value ELSE 0 END) user_total
    , CURRENT_TIMESTAMP AS inserted_at
    , 'dbt' AS inserted_by
FROM {{ source('core', 'embrace_metrics_log') }}

{% if is_incremental() %}
    WHERE (date_utc >= (SELECT MAX(date_utc) FROM {{ this }}) - INTERVAL '1 day')
{% endif %}

GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6 DESC