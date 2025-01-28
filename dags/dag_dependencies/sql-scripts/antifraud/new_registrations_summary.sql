-- Create a high level summary of new users in first 3 days of registration for fraud
-- this tries to summarize activity from multiple sources for accounts created on mobile clients
INSERT INTO core.registration_fraud_summary
WITH reg_users AS (
    SELECT
        username,
        created_at,
        country_code,
        client_type,
        provider,
        client_ip,
        client_version,
        adid,
        account_status,
        user_id_hex
    FROM core.registrations
    JOIN core.users USING (username)
    WHERE
        (DATE(created_at) = '{{ macros.ds_add(ds, -2) }}')
        AND (client_type IN ('TN_ANDROID', 'TN_IOS_FREE', '2L_ANDROID'))
),
embrace_summary AS (
    SELECT
        username,
        COUNT(DISTINCT session_id) num_embrace_sessions,
        COUNT(CASE WHEN name ='tap' THEN 1 ELSE 0 END) num_embrace_taps,
        COUNT(CASE WHEN name = 'view-start' THEN 1 ELSE 0 END) num_embrace_views,
        MAX(ts) - MIN(ts) embrace_time_diff,
        MIN(ts) embrace_earliest_event_ts
    FROM core.embrace_data
    JOIN reg_users ON user_id=user_id_hex
    WHERE
        (received_ts BETWEEN DATE_PART(epoch_second, '{{ macros.ds_add(ds, -2) }}'::TIMESTAMP) - 86400
        AND DATE_PART(epoch_second, '{{ macros.ds_add(ds, -2) }}'::TIMESTAMP) + 86400 * 3)
   GROUP BY 1
),
ads AS (
    SELECT
        "client_details.client_data.user_data.username" AS username,
        COUNT(*) AS num_ads
    FROM party_planner_realtime.adshoweffective_combined_data
    WHERE
        (date_utc BETWEEN '{{ macros.ds_add(ds, -2) }}'::DATE AND '{{ ds }}'::DATE)
        AND ("client_details.client_data.client_platform" IN ('IOS', 'CP_ANDROID'))
        AND ("payload.ad_event_data.network" != 'AD_NETWORK_IN_HOUSE')
    GROUP BY 1
),
applifecycle_metrics AS (
    SELECT username,
        COUNT(*) num_applifecyle_events,
        SUM(LEAST(600, DATEDIFF('seconds', event_time, next_event_time))) total_duration_applifecycle,
        COUNT(DISTINCT client_country_code) num_applifecycle_countries
    FROM analytics_staging.applifecycle_events
    WHERE
        (event='APP_LIFECYCLE_FOREGROUNDED')
        AND (next_event='APP_LIFECYCLE_BACKGROUNDED')
        AND date_utc BETWEEN '{{ macros.ds_add(ds, -2) }}' AND '{{ ds }}'::DATE
    GROUP BY 1
)

SELECT
    username,
    created_at,
    country_code,
    client_type,
    provider,
    client_ip,
    client_version,
    adid,
    account_status,
    user_id_hex,
    num_embrace_sessions,
    num_embrace_taps,
    num_embrace_views,
    embrace_time_diff,
    embrace_earliest_event_ts,
    num_ads,
    NULL AS num_lp_sessions,
    NULL AS lp_duration,
    NULL AS num_lp_country,
    num_applifecyle_events,
    total_duration_applifecycle,
    num_applifecycle_countries
FROM reg_users
LEFT JOIN embrace_summary USING (username)
LEFT JOIN ads USING (username)
LEFT JOIN applifecycle_metrics USING (username);
