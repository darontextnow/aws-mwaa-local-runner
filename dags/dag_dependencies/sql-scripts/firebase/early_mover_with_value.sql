WITH pp_events AS (
    SELECT DISTINCT
        "client_details.android_bonus_data.adjust_id" adid,
        "client_details.android_bonus_data.google_analytics_unique_id" gaid
    FROM party_planner_realtime.applifecyclechanged
    WHERE
        (date_utc >= '{{ ds }}'::DATE - INTERVAL '2 DAYS')
        AND (adid IS NOT NULL)
        AND (gaid IS NOT NULL)
        AND (gaid != '')
        AND ("client_details.client_data.client_platform" = 'CP_ANDROID')
),
pred AS (
    SELECT
        a.adjust_id AS adid,
        ANY_VALUE(a.installed_at)::DATE AS install_date,
        MAX(a.pred_active_days) AS pred_active_days,
        WIDTH_BUCKET(MAX(pred_active_days), 9, 26, 5) AS bin_number
    FROM analytics.ltv_d1_prediction a
    JOIN analytics_staging.ua_early_mover_event b ON (a.adjust_id = b.adjust_id AND a.client_type = b.client_type)
    WHERE
        (a.installed_at >= '{{ ds }}'::DATE - INTERVAL '2 DAYS')
        AND ((outbound_call = 1) OR (lp_sessions = 1)) -- OR condition is neccessary
        AND (a.client_type = 'TN_ANDROID')
    GROUP BY 1
),
pred_binned AS (
    SELECT
        bin_number,
        AVG(pred_active_days) AS bin_avg
    FROM pred
    GROUP BY 1
)
SELECT
    pp_events.gaid AS "gaid",
    'USD' AS "currency",
    pred_binned.bin_avg * 1.63 * margin_android_us_365 AS "value"  -- 1.63 is AD90 to AD365 conversion factor
FROM pred
JOIN pred_binned ON (pred.bin_number = pred_binned.bin_number)
JOIN pp_events ON (pred.adid = pp_events.adid)
JOIN analytics.ua_margin_per_active_day_by_geo ON LAST_DAY(install_date, month) = year_month
LEFT JOIN analytics_staging.ua_events_sent_to_firebase b ON (pp_events.gaid = b.gaid)
          AND (b.event_name = 'early_mover_with_value')
WHERE (b.gaid IS NULL)
