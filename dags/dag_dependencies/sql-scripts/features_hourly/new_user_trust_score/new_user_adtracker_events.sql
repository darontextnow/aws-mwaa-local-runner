INSERT INTO core.new_user_adtracker_events
SELECT
    "client_details.client_data.user_data.username" AS username,
    COALESCE("client_details.android_bonus_data.adjust_id", "client_details.ios_bonus_data.adjust_id") AS adid,
    a.created_at AS timestamp,
    'ad_show_effective' AS type,
    CASE
        WHEN "client_details.client_data.client_platform" = 'IOS' THEN 'TN_IOS_FREE'
        WHEN ("client_details.client_data.client_platform" = 'CP_ANDROID') AND
            ("client_details.client_data.brand" = 'BRAND_2NDLINE') THEN '2L_ANDROID'
        WHEN ("client_details.client_data.client_platform" = 'CP_ANDROID') AND
            ("client_details.client_data.brand" = 'BRAND_TEXTNOW') THEN 'TN_ANDROID'
        WHEN "client_details.client_data.client_platform" = 'WEB' THEN 'TN_WEB'
        ELSE 'OTHER'
    END AS client_type,
    "client_details.client_data.client_version" AS client_version,
    "payload.ad_event_data.advertisement.size.width" || 'x' || "payload.ad_event_data.advertisement.size.height" AS ad_format,
    NULL AS ad_id,
    NULL AS ad_name,
    "payload.ad_event_data.network" AS ad_network,
    "payload.ad_event_data.placement" AS ad_placement,
    NULL AS ad_platform,
    "payload.ad_event_data.request_type" AS ad_type,
    "payload.ad_event_data.ad_unit_id" AS line_item_id,
    '{{ execution_date }}'::TIMESTAMP
FROM (
    SELECT *
    FROM party_planner_realtime.adshoweffective_combined_data
    WHERE
        (date_utc = '{{ ds }}'::DATE) --first take advantage of clustered column pruning.
        AND (created_at BETWEEN '{{ execution_date }}'::TIMESTAMP - INTERVAL '1 HOUR' AND '{{ execution_date }}'::TIMESTAMP + INTERVAL '1 HOUR')
) a
JOIN (
    SELECT DISTINCT username, event_timestamp
    FROM core.new_user_snaphot
    WHERE (execution_time = '{{ execution_date }}'::TIMESTAMP)
) b ON a."client_details.client_data.user_data.username" = b.username
JOIN core.users c ON a."client_details.client_data.user_data.username" = c.username
WHERE
    (c.account_status = 'ENABLED')
    AND (a.created_at < b.event_timestamp + INTERVAL '1 HOUR')
;
