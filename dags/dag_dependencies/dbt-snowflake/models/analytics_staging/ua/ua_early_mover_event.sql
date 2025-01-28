{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='SURROGATE_KEY'
    )
}}


WITH filtered_pp_sessions AS (
    SELECT
        created_at,
        "client_details.client_data.user_data.username" AS username
    FROM {{ source('party_planner_realtime', 'app_lifecycle_changed') }}
    WHERE
        {% if is_incremental() %}
            (created_at::DATE BETWEEN {{ var('ds') }}::DATE - INTERVAL '7 days' AND {{ var('ds') }}::DATE)
        {% else %}
            (created_at::DATE BETWEEN '2024-07-01'::DATE AND {{ var('ds') }}::DATE)
        {% endif %}
        AND ("client_details.client_data.country_code" IN ('US', 'CA'))
        AND "payload.time_in_session" > 10000 --condition for true sessions with more than 10 secs
        AND ("client_details.client_data.client_platform" IN ('CP_ANDROID', 'IOS'))
        AND LENGTH("client_details.client_data.user_data.username") > 0 --capture only non-empty users
),
pp_sessions AS (
    SELECT
        b.adid,
        d.installed_at,
        CASE WHEN d.app_id IN ('com.enflick.android.TextNow') THEN 'TN_ANDROID'
             WHEN d.app_id = '314716233' THEN 'TN_IOS_FREE' ELSE d.app_id END AS app_client_type,
        ip_address,
        COUNT(*) AS num_sessions
    FROM  filtered_pp_sessions a
    JOIN {{ ref('registrations_1') }} b ON a.username = b.username
    JOIN {{ source('core', 'users') }} c ON a.username = c.username
    JOIN {{ ref('installs_with_pi') }} d ON b.adid = d.adid
    WHERE
        {% if is_incremental() %}
            d.installed_at::DATE BETWEEN {{ var('ds') }}::DATE - INTERVAL '7 days' AND {{ var('ds') }}::DATE
            AND b.created_at::DATE BETWEEN {{ var('ds') }}::DATE - INTERVAL '7 days' AND {{ var('ds') }}::DATE
        {% else %}
            b.created_at BETWEEN '2024-07-01'::DATE AND {{ var('ds') }}::DATE
            AND d.installed_at BETWEEN '2024-07-01'::DATE AND {{ var('ds') }}::DATE
        {% endif %}
        AND a.created_at <= b.created_at + INTERVAL '24 hours'
        AND c.account_status = 'ENABLED'
        AND b.country_code IN ('US','CA')
        AND d.country IN ('us','ca')
        AND b.tracker_name NOT LIKE 'Untrusted%'
        AND d.app_id IN ('com.enflick.android.TextNow','314716233')
    GROUP BY 1, 2, 3, 4
    HAVING (num_sessions >= 13 and app_client_type = 'TN_ANDROID')
        OR (num_sessions >= 10 and app_client_type = 'TN_IOS_FREE')
),
devices_with_calls AS (
    SELECT
        c.adid,
        CASE
            WHEN d.app_id = 'com.enflick.android.TextNow' THEN 'TN_ANDROID'
            WHEN d.app_id = '314716233' THEN 'TN_IOS_FREE' ELSE d.app_id
        END AS client_type,
        d.installed_at,
        d.ip_address
    FROM {{ source('party_planner_realtime', 'call_completed') }} a
    JOIN {{ source('core', 'users') }} b ON a.user_id_hex = b.user_id_hex
    JOIN {{ ref('registrations_1') }} c ON b.username = c.username
    JOIN {{ ref('installs_with_pi') }} d ON c.adid = d.adid
    JOIN {{ source('adjust', 'apps') }} app ON d.APP_ID = app.app_id AND d.app_name = app.app_name
    WHERE
        {% if is_incremental() %}
            d.installed_at::DATE BETWEEN {{ var('ds') }}::DATE - INTERVAL '7 days' AND {{ var('ds') }}::DATE
            AND a.created_at::DATE BETWEEN {{ var('ds') }}::DATE - INTERVAL '7 days' AND {{ var('ds') }}::DATE
            AND c.created_at::DATE BETWEEN {{ var('ds') }}::DATE - INTERVAL '7 days' AND {{ var('ds') }}::DATE
        {% else %}
            a.created_at BETWEEN '2024-07-01'::DATE AND {{ var('ds') }}::DATE
            AND c.created_at BETWEEN '2024-07-01'::DATE AND {{ var('ds') }}::DATE
            AND d.installed_at BETWEEN '2024-07-01'::DATE AND {{ var('ds') }}::DATE
        {% endif %}
        AND a.created_at <= c.created_at + INTERVAL '24 hours'
        AND "payload.call_duration.seconds" > 10
        AND "payload.call_direction" LIKE '%OUTBOUND%'
        AND account_status = 'ENABLED'
        AND c.country_code IN ('US','CA')
        AND d.country IN ('us','ca')
        AND "client_details.client_data.client_platform" IN ('CP_ANDROID','IOS')
        AND c.tracker_name NOT LIKE 'Untrusted%'
        AND d.app_id IN ('com.enflick.android.TextNow','314716233')
),
pp_sessions_outbound_call_combined AS (
    SELECT
      adid,
      app_client_type AS client_type,
      installed_at,
      ip_address
    FROM pp_sessions
    UNION SELECT
      adid,
      client_type,
      installed_at,
      ip_address
    FROM devices_with_calls
)

SELECT DISTINCT
    adid AS adjust_id,
    client_type,
    installed_at,
    {{ dbt_utils.generate_surrogate_key(['adid', 'client_type','installed_at']) }} AS surrogate_key,
    outbound_call,
    pp_sessions AS lp_sessions,
    CASE WHEN pp_sessions = 1 AND outbound_call = 1 THEN 1 ELSE 0 END AS combined_event,
    ip_address
FROM (
    SELECT
        c.adid,
        c.client_type,
        c.installed_at,
        CASE WHEN pp.adid IS NOT NULL THEN 1 ELSE 0 END AS pp_sessions,
        CASE WHEN dc.adid IS NOT NULL THEN 1 ELSE 0 END AS outbound_call,
        c.ip_address
    FROM pp_sessions_outbound_call_combined c
    LEFT OUTER JOIN pp_sessions pp
        ON c.adid = pp.adid
        AND c.installed_at = pp.installed_at
        AND c.client_type = pp.app_client_type
    LEFT OUTER JOIN devices_with_calls dc
        ON c.adid = dc.adid
        AND c.installed_at = dc.installed_at
        AND c.client_type = dc.client_type
)
