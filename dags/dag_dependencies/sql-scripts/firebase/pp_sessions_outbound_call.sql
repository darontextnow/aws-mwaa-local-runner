WITH pp_events AS (
    SELECT DISTINCT
        "client_details.android_bonus_data.adjust_id" adid,
        "client_details.android_bonus_data.google_analytics_unique_id" gaid,
        created_at
    FROM party_planner_realtime.applifecyclechanged
    WHERE
        (date_utc >= '{{ ds }}'::DATE - INTERVAL '2 DAYS')
        AND (adid IS NOT NULL)
        AND (gaid IS NOT NULL)
        AND (gaid != '')
),
early_mover AS (
    SELECT DISTINCT adjust_id AS adid
    FROM analytics_staging.ua_early_mover_event a
    WHERE
        (installed_at >= '{{ ds }}'::DATE - INTERVAL '2 DAYS')
        AND (combined_event = 1) --- meets both outbound calls and session condition
        AND (client_type = 'TN_ANDROID')
)

SELECT new_gaids.gaid AS "gaid"
FROM (
    SELECT DISTINCT LOWER(pp_events.gaid) AS gaid
    FROM early_mover
    JOIN pp_events ON (early_mover.adid = pp_events.adid)
) new_gaids
