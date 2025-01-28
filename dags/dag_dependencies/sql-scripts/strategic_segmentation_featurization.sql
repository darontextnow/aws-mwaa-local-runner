WITH user_info AS (
SELECT
    COALESCE(dau.set_uuid, rfm.set_uuid) AS user_set_id,
    frequency,
    recency,
    user_age AS tenure_days,
    COUNT(1) AS active_username_ct
FROM (
    SELECT DISTINCT username
    FROM dau.user_device_master
    WHERE (date_utc >= '{{ ds }}')
        AND (date_utc < '{{ data_interval_end.date() }}')
    ) udm
    JOIN dau.user_set dau ON (udm.username = dau.username)
    LEFT JOIN core.rfm_segments rfm ON (rfm.set_uuid = dau.set_uuid)
WHERE
    (dau.set_uuid NOT IN (SELECT set_uuid FROM dau.bad_sets))
    AND (rfm.run_date = '{{ macros.ds_add(data_interval_end.date(), -1) }}')
GROUP BY 1, 2, 3, 4
),
total_user_count AS (
SELECT set_uuid, COUNT(*) AS total_username_ct
FROM dau.user_set
GROUP BY 1
),
sim_info AS (
SELECT DISTINCT user_set_id
FROM analytics_staging.user_segment_user_set_daily_tn_type
WHERE
    (date_utc >= '{{ ds }}')
    AND (date_utc < '{{ data_interval_end.date() }}')
    AND (sub_type IN ('free/no data', 'free/with data', 'paid/no data',
                      'paid/with data', 'TN Employee/with data'))

),
engagement_info AS (
SELECT
    user_set_id,
    SUM(ad_impressions) AS ad_impressions,
    SUM(mms_messages) AS mms_messages,
    SUM(sms_messages) AS sms_messages,
    SUM(total_outgoing_calls) AS out_calls
FROM analytics.user_set_daily_activities
WHERE
    (date_utc >= '{{ ds }}')
    AND (date_utc < '{{ data_interval_end.date() }}')
    AND (client_type IN ('TN_ANDROID', '2L_ANDROID', 'TN_IOS_FREE'))
GROUP BY user_set_id
)
SELECT
    ui.user_set_id,
    recency,
    frequency,
    tenure_days,
    COALESCE(ad_impressions, 0) AS impressions_ct,
    si.user_set_id IS NOT NULL AS sim_presence,
    COALESCE(sms_messages, 0) + COALESCE(mms_messages, 0) AS messages_ct,
    COALESCE(out_calls, 0) AS calls_ct,
    ui.active_username_ct,
    tuc.total_username_ct
FROM user_info ui
LEFT JOIN sim_info si ON (ui.user_set_id = si.user_set_id)
LEFT JOIN engagement_info ei ON (ui.user_set_id = ei.user_set_id)
LEFT JOIN total_user_count tuc ON (ui.user_set_id = tuc.set_uuid)
