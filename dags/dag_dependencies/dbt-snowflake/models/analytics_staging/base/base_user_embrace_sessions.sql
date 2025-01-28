{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE',
        enabled=false
    )
}}

WITH cte_get_all_session AS (
    SELECT DISTINCT
        session_id,
        username,
        device_id,
        CASE WHEN app_id = 'DiFBd' THEN 'TN_ANDROID'
             WHEN app_id = 'NUe92' THEN '2L_ANDROID'
             WHEN app_id = '53VwF' THEN 'TN_IOS_FREE'
        END AS client_type,
        name AS event_name,
        country,
        TO_TIMESTAMP(received_ts)::DATE AS date_utc,
        TO_TIMESTAMP(ts) AS event_ts,
        details:location.x AS location_a,
        details:location.y AS location_b,
        details:state AS state,
        details AS details
    FROM {{ source('core', 'embrace_data') }} a
    INNER JOIN {{ source('core', 'users') }} c ON a.user_id = c.user_id_hex
    WHERE
        (c.account_status NOT IN ('HARD_DISABLED', 'DISABLED'))
        AND (TO_DATE(TO_TIMESTAMP(received_ts)) <= {{ var('ds') }}::DATE)
    {% if is_incremental() %}
        AND (TO_DATE(TO_TIMESTAMP(received_ts)) >= {{ var('ds') }}::DATE  - INTERVAL '1 days')
    {% else %}
        AND (TO_DATE(TO_TIMESTAMP(received_ts)) >= '2024-04-20')
    {% endif %}
),
--foreground sessions with tap or view-start after the session start are included as valid session
cte_get_valid_foreground_session_start AS (
    SELECT DISTINCT
        a.session_id,
        a.username,
        a.event_name,
        a.device_id,
        a.client_type,
        a.country,
        a.date_utc,
        a.event_ts AS event_ts
    FROM cte_get_all_session a
    INNER JOIN cte_get_all_session b ON
        (a.session_id = b.session_id
        AND a.username = b.username)
        AND (b.event_ts >= a.event_ts)
    WHERE
        (a.state = 'foreground')
        AND (a.event_name = 'session-start')
        AND (b.event_name IN ('view-start', 'tap'))
),
--sessions can end with various events,the event which occured first is considered
get_valid_session_start_and_end AS (
    SELECT
        a.session_id,
        a.username,
        a.event_name,
        device_id,
        a.client_type,
        a.country,
        a.date_utc,
        a.event_ts
    FROM  cte_get_valid_foreground_session_start a
    UNION ALL SELECT DISTINCT
        a.session_id,
        a.username,
        b.event_name,
        a.device_id,
        a.client_type,
        a.country,
        b.date_utc,
        b.event_ts
    FROM  cte_get_valid_foreground_session_start a
    INNER JOIN (
        SELECT
            session_id,
            username,
            event_name,
            client_type,
            country,
            date_utc,
            event_ts
        FROM cte_get_all_session
        WHERE
            (event_name IN ('session-end-normal', 'session-end', 'session_end_crash', 'session_end_oom', 'session_end_user_terminated', 'session_end_anr'))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY username, session_id ORDER BY TO_TIMESTAMP(event_ts)) = 1
    )b ON a.session_id = b.session_id AND a.username = b.username
)

SELECT * FROM (
    SELECT
        a.date_utc,
        a.username,
        a.device_id,
        a.client_type,
        a.country,
        a.session_id,
        a.event_ts::DATE AS session_date,
        a.event_ts AS session_start_timestamp,
        LEAD(a.event_ts, 1, a.event_ts) OVER (PARTITION BY a.session_id, a.username ORDER BY a.event_ts ) AS session_end_timestamp,
            --limit session duration to 30 minutes as users sometime have their app in foreground running for long period with their screen auto-lock set to never
        CASE WHEN DATEDIFF(SECOND, session_start_timestamp, session_end_timestamp) > 1800 THEN 1800
             ELSE DATEDIFF(SECOND, session_start_timestamp, session_end_timestamp)
             END AS duration,
        b.location_a AS location_first_octet_ip,
        b.location_b AS location_second_octet_ip
        FROM get_valid_session_start_and_end  a
            -- Most recent location within a session has been captured
        LEFT OUTER JOIN (
            SELECT session_id, location_a, location_b
            FROM cte_get_all_session
            WHERE location_b IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY username, session_id ORDER BY TO_TIMESTAMP(event_ts)) = 1
        )b ON a.session_id = b.session_id
)f
WHERE (duration > 0)

