WITH pp_events AS (
    SELECT DISTINCT
        NULLIF(TRIM("client_details.android_bonus_data.adjust_id"), '') adid,
        NULLIF(TRIM("client_details.android_bonus_data.google_analytics_unique_id"), '') gaid,
        created_at
    FROM party_planner_realtime.applifecyclechanged
    WHERE
        (date_utc = '{{ ds }}'::DATE)
        AND (adid IS NOT NULL)
        AND (gaid IS NOT NULL)
),

ltv_d1 AS (
    SELECT adjust_id AS adid
    FROM analytics.ltv_d1_prediction a
    WHERE
        (a.installed_at >= (SELECT MAX(installed_at) FROM analytics.ltv_d1_prediction WHERE installed_at::DATE = '{{ ds }}'::DATE) - INTERVAL '1 DAY')
        AND (a.installed_at < (SELECT MAX(installed_at) FROM analytics.ltv_d1_prediction WHERE installed_at::DATE = '{{ ds }}'::DATE))
        AND (client_type = 'TN_ANDROID')
        AND (a.proba >= .33)
)

SELECT DISTINCT gaid AS "gaid"
FROM pp_events a
INNER JOIN ltv_d1 b ON (a.adid = b.adid);
