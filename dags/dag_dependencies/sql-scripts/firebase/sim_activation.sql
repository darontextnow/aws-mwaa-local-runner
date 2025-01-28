WITH pp_events AS (
    SELECT DISTINCT
        COALESCE(
            NULLIF(TRIM("client_details.android_bonus_data.adjust_id"), ''),
            NULLIF(TRIM("client_details.ios_bonus_data.adjust_id"), '')
        ) adid,
        COALESCE(
            NULLIF(TRIM("client_details.android_bonus_data.google_analytics_unique_id"), ''),
            NULLIF(TRIM("client_details.ios_bonus_data.google_analytics_unique_id"), '')
        ) gaid,
        created_at
    FROM party_planner_realtime.applifecyclechanged
    WHERE
        (date_utc >= '{{ ds }}'::DATE - INTERVAL '7 DAYS')
        AND (adid IS NOT NULL)
        AND (gaid IS NOT NULL)
)

SELECT DISTINCT LOWER(pp_events.gaid) AS "gaid"
FROM (SELECT adid FROM adjust.sim_activation WHERE (DATE(created_at) = '{{ ds }}'::DATE)) adids
JOIN pp_events ON (pp_events.adid = adids.adid)
QUALIFY RANK() OVER (PARTITION BY pp_events.adid ORDER BY pp_events.created_at DESC) = 1
