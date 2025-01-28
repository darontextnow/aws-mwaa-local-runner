WITH pp_sessions_and_outbound_call AS (
    SELECT DISTINCT
       adjust_id AS adid,
       'com.enflick.android.TextNow' AS app_id,
       installed_at
    FROM analytics_staging.ua_early_mover_event a
    WHERE
       (installed_at >= '{{ ds }}'::DATE - INTERVAL '2 days')
       AND (combined_event = 1) --- meets both outbound calls and session condition
       AND (a.client_type = 'TN_ANDROID') ---only Android devices campaign is sent as per LP version's logic
)

SELECT DISTINCT
    'fnz52pd8uj2r' AS app_token,
    b.app_id,
    b.adid,
   '29aaw2' AS event_token,
    android_id,
    gps_adid,
    idfa,
    idfv,
    ip_address,
    --  following day's run date's end of the day timestamp,so that the send timestamp > first open session time of an event
    EXTRACT(EPOCH_SECONDS FROM DATEADD(ms, -3, '{{ macros.ds_add(ds, 2) }}') ) AS created_at_unix
FROM pp_sessions_and_outbound_call a
INNER JOIN adjust.installs_with_pi b ON
    (a.adid = b.adid)
    AND (a.app_id = b.app_id)
    AND (a.installed_at = b.installed_at)
