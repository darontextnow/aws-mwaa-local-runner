/* generate the GAIDs that have verfied regsitration event */

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
        "client_details.client_data.client_platform" platform
    FROM party_planner_realtime.applifecyclechanged
    WHERE
        (date_utc >= '{{ ds }}'::DATE - INTERVAL '2 DAYS')
        AND (adid IS NOT NULL)
        AND (gaid IS NOT NULL)
), 

adids AS (
    SELECT DISTINCT i.adid
    FROM adjust.installs i
    JOIN adjust.apps a USING (app_id, app_name)
    JOIN adjust.registrations ar USING (adid, app_id, app_name)
    JOIN core.users cr USING (username)
    WHERE (DATE(cr.created_at) = '{{ ds }}'::DATE)
)

SELECT DISTINCT gaid AS "gaid", platform
FROM pp_events
JOIN adids ON (pp_events.adid = adids.adid)
