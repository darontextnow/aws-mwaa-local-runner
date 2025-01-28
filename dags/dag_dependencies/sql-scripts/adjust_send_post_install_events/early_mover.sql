WITH early_mover_event AS (
    SELECT DISTINCT
       adjust_id AS adid,
       CASE WHEN client_type='TN_ANDROID' THEN 'com.enflick.android.TextNow'
            WHEN client_type='TN_IOS_FREE' THEN '314716233'
       END AS app_id,
       installed_at
    FROM analytics_staging.ua_early_mover_event a
    LEFT OUTER JOIN analytics_staging.ua_events_sent_to_adjust b ON (a.adjust_id = b.adid)
    WHERE
       (installed_at >= '{{ ds }}'::DATE - INTERVAL '2 days')
       AND ((outbound_call = 1) OR (lp_sessions = 1)) -- OR condition is neccessary
       AND (b.adid IS NULL)
)

SELECT DISTINCT
    CASE a.app_id
        WHEN 'com.enflick.android.TextNow' THEN 'fnz52pd8uj2r'
        WHEN '314716233' THEN 'szvvsxe5b4t2'
    END AS app_token,
    a.app_id,
    a.adid,
    CASE a.app_id
        WHEN 'com.enflick.android.TextNow' THEN 'e02ylj'
        WHEN '314716233' THEN '7dnrfp'
    END AS event_token,
    android_id,
    gps_adid,
    idfa,
    idfv,
    ip_address,
    --  following day's run date's end of the day timestamp,so that the send timestamp > first open session time of an event
    EXTRACT(EPOCH_SECONDS FROM DATEADD(ms, -3, '{{ macros.ds_add(ds, 2) }}') ) AS created_at_unix
FROM early_mover_event a
INNER JOIN adjust.installs_with_pi b ON
    (a.adid = b.adid)
    AND (a.app_id = b.app_id)
    AND (a.installed_at = b.installed_at)
