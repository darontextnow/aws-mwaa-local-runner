-- get accounts created on mobile over last week with a buffer of 3 days that have very little activty
-- seen on mobile sdks like embrace, applifecycle events and adtracker events
-- not coming from paid acquisition sources
WITH accts AS (
    SELECT a.*, r.IDFV, c.is_organic, c.is_untrusted, d.set_uuid
    FROM core.registration_fraud_summary a
    JOIN core.users b ON (a.username = b.username)
    JOIN core.registrations r ON (a.username = r.username)
    LEFT JOIN analytics.installs c ON (a.adid=adjust_id) AND (a.client_type=c.client_type)
    LEFT JOIN dau.user_set d ON (a.username=d.username)
    LEFT JOIN analytics.user_sets e ON (d.set_uuid=e.user_set_id)
    WHERE
        (a.created_at BETWEEN (CURRENT_DATE - INTERVAL '7 days') AND (CURRENT_DATE - INTERVAL '3 days'))
        AND (
            ((NVL(num_embrace_sessions, 0) <= 1) OR (NVL(embrace_time_diff, 0) < 120))
            AND (NVL(num_ads, 0) <= 1)
            AND (NVL(num_applifecyle_events, 0)<=2)
            AND (NVL(total_duration_applifecycle, 0)<=120)
            AND (b.account_status != 'HARD_DISABLED')
            AND (NVL(c.is_organic, TRUE) = TRUE)
        )
),

not_disabled_sets AS (
    SELECT DISTINCT set_uuid
    FROM accts
    WHERE (set_uuid NOT IN (SELECT set_uuid FROM dau.bad_sets))
),

-- get users who could belong to potentially good user clusters
good_sets AS (
    SELECT
        user_set_id,
        COUNT(DISTINCT a.username) num_users,
        COUNT(DISTINCT c.username) num_paying_users,
        COUNT(DISTINCT date_utc) num_days_active,
        SUM(ad_impressions) num_ads,
        SUM(total_outgoing_calls) out_call,
        SUM(sms_messages + mms_messages) out_msg,
        AVG(total_outgoing_call_duration) avg_out_call_dur,
        AVG(total_incoming_call_duration) avg_in_call_dur
    FROM analytics.user_daily_activities a
    JOIN (
        SELECT username, user_set_id
        FROM analytics.users
        WHERE (user_set_id IN (SELECT set_uuid FROM not_disabled_sets))
    ) b ON (a.username = b.username)
    LEFT JOIN trust_safety.paying_users_30d c ON (a.username = c.username)
    WHERE
        (date_utc > CURRENT_DATE - INTERVAL '31 days')
        AND (a.client_type != 'OTHER')
    GROUP BY 1
    HAVING
        (num_ads/num_users > 10)
        OR (avg_out_call_dur + avg_in_call_dur > 30)
        OR (num_paying_users > 0)
),

-- get users who are active in calling over mobile, they can be trusted more than users who just send messages
calling_active_users AS (
    SELECT
        a.username,
        SUM(ad_impressions) total_ad_impressions,
        SUM(CASE WHEN a.client_type NOT IN ('TN_WEB','OTHER') THEN total_outgoing_calls ELSE 0 END) out_calls_mobile,
        SUM(CASE WHEN a.client_type NOT IN ('TN_WEB','OTHER') THEN total_outgoing_call_duration ELSE 0 END) out_call_duration_mobile,
        SUM(CASE WHEN a.client_type NOT IN ('TN_WEB','OTHER') THEN total_incoming_calls ELSE 0 END) in_calls_mobile,
        SUM(CASE WHEN a.client_type NOT IN ('TN_WEB','OTHER') THEN total_incoming_call_duration ELSE 0 END) in_call_duration_mobile,
        SUM(total_outgoing_calls) out_calls,
        SUM(total_outgoing_call_duration) out_call_duration,
        SUM(total_incoming_calls) in_calls,
        SUM(total_incoming_call_duration) in_call_duration,
        SUM(sms_messages + mms_messages) total_messages
    FROM accts a
    JOIN analytics.user_daily_activities b ON (a.username = b.username)
    WHERE (date_utc > CURRENT_DATE - INTERVAL '4 days')
    GROUP BY 1
    HAVING
        (out_calls_mobile > 2)
        AND ((out_call_duration_mobile::FLOAT/out_calls_mobile > 30) OR (total_messages < out_calls * 2))
),

-- get all applifecycle events that contain device ids.
-- we will use this to filter devices that show high activity per user
applifecycle_events_30days AS (
    SELECT * 
    FROM prod.analytics_staging.applifecycle_events
    WHERE
        (event = 'APP_LIFECYCLE_FOREGROUNDED')
        AND (next_event = 'APP_LIFECYCLE_BACKGROUNDED')
        AND date_utc BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE
),

new_user_adjust_id_filter AS (
    SELECT 
        adjust_id,
        SUM(LEAST(600, DATEDIFF('seconds', event_time, next_event_time))) all_users_total_duration_applifecycle,
        COUNT(DISTINCT a.username) num_users
    FROM applifecycle_events_30days a
    JOIN accts b ON adid = adjust_id
    GROUP BY 1
    HAVING all_users_total_duration_applifecycle::FLOAT/num_users > 300
),

new_user_idfv_filter AS (
    SELECT 
        idfv,
        SUM(LEAST(600, DATEDIFF('seconds', event_time, next_event_time))) all_users_total_duration_applifecycle,
        COUNT(DISTINCT a.username) num_users
    FROM applifecycle_events_30days a
    JOIN accts b USING (idfv)
    GROUP BY 1
    HAVING all_users_total_duration_applifecycle::FLOAT/num_users > 300
)

-- get users for disabling
-- exclude users who meet filters above or have ordered sim card
SELECT DISTINCT a.username
FROM accts a
LEFT JOIN good_sets ON (user_set_id = set_uuid)
LEFT JOIN inventory.orders_data orders ON (a.username = orders.username)
LEFT JOIN calling_active_users ca ON (a.username = ca.username)
LEFT JOIN new_user_adjust_id_filter adj ON (a.adid = adj.adjust_id)
LEFT JOIN new_user_idfv_filter ios ON (a.idfv = ios.idfv)
WHERE
    (user_set_id IS NULL)
    AND (orders.username IS NULL)
    AND (ca.username IS NULL)
    AND (adj.adjust_id IS NULL)
    AND (ios.idfv IS NULL)
    AND (a.username NOT IN (SELECT username FROM public.username_exclusion_list_ts))
