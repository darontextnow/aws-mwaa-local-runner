{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

{% if is_incremental() %}
  {% set query %}
    SELECT DATE_PART(EPOCH, {{ var('ds') }}::DATE - INTERVAL '3 days')
  {% endset %}

  {% if execute %}
      {% set max_dt = run_query(query).columns[0][0] %}
  {% else %}
      {% set max_dt = 1717200000 %}
  {% endif %}
{% endif %}


WITH userlevel_embrace_data AS (
    SELECT
        TO_DATE(TO_TIMESTAMP(received_ts)) AS date_utc,
        user_id,
        CASE
            WHEN app_id = 'DiFBd' THEN 'TN_ANDROID'
            WHEN app_id = 'NUe92' THEN '2L_ANDROID'
            WHEN app_id = '53VwF' THEN 'TN_IOS_FREE'
        END AS client_type,
        model AS device,
        manufacturer,
        app_version,
        os_version,
        COUNT(session_id) AS session_cnt,
        SUM(CASE WHEN name = 'session-end-anr' THEN 1 ELSE 0 END) AS count_anr,
        SUM(CASE WHEN name = 'session-end-crash' THEN 1 ELSE 0 END) AS count_crash
    FROM {{ source('core', 'embrace_data') }}
    WHERE
        received_ts >= 1717200000 -- Jun 1,2024 00:00:00
    {% if is_incremental() %}
        AND received_ts >= {{ max_dt }}
    {% endif %}
        AND user_id IS NOT NULL
        AND user_id != ''
        AND name IN ('session-end-anr', 'session-end-crash', 'session-end-normal',
                    'session-end-oom', 'session-end-user-terminated')
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),
agg_user_with_flags AS (
    SELECT
        date_utc,
        user_id,
        client_type,
        manufacturer,
        device,
        app_version,
        os_version,
        SUM(session_cnt) AS ttl_sessions,
        SUM(count_anr) AS anr_sessions,
        SUM(count_crash) AS crash_sessions,
        CASE
            WHEN (anr_sessions > 0 AND crash_sessions > 0) THEN anr_sessions + crash_sessions
            ELSE 0
        END AS crash_anr_sessions,
        CASE WHEN SUM(count_anr) > 0 THEN 1 ELSE 0 END AS anr_user_flag,
        CASE WHEN SUM(count_crash) > 0 THEN 1 ELSE 0 END AS crash_user_flag,
        CASE WHEN crash_sessions > 0 THEN 1 ELSE 0 END AS crash_anr_user_flag
    FROM userlevel_embrace_data
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)
SELECT
    date_utc,
    client_type,
    app_version,
    manufacturer,
    device,
    os_version,
    COUNT(user_id) AS total_users,
    SUM(ttl_sessions) AS total_sessions,
    -- ANR Metrics
    SUM(anr_sessions) AS total_anr_sessions,
    NULLIF(SUM(anr_sessions), 0) / NULLIF(SUM(ttl_sessions), 0) AS anr_rate,
    1 - anr_rate AS anr_free_session_rate,
    COUNT(DISTINCT CASE WHEN anr_user_flag = 1 THEN user_id END) AS total_anr_users,
    total_users - total_anr_users AS total_anr_free_users,

    -- Crash Metrics
    SUM(crash_sessions) AS total_crash_sessions,
    NULLIF(SUM(crash_sessions), 0) / NULLIF(SUM(ttl_sessions), 0) AS crash_rate,
    1 - crash_rate AS crash_free_session_rate,
    COUNT(DISTINCT CASE WHEN crash_user_flag = 1 THEN user_id END) AS total_crash_users,
    total_users - total_crash_users AS total_crash_free_users,

    -- Combined Metrics
    SUM(crash_anr_sessions) AS total_crash_anr_sessions,
    NULLIF(SUM(crash_anr_sessions), 0) / NULLIF(SUM(ttl_sessions), 0) AS crash_anr_rate,
    1 - (SUM(crash_anr_sessions)/SUM(ttl_sessions)) AS crash_anr_free_session_rate,
    COUNT(DISTINCT CASE WHEN crash_anr_user_flag = 1 THEN user_id END) AS total_crash_anr_users,
    total_users - total_crash_anr_users AS total_crash_anr_free_users,

    CURRENT_TIMESTAMP AS inserted_at,
    'dbt' AS inserted_by
FROM agg_user_with_flags
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1 DESC
