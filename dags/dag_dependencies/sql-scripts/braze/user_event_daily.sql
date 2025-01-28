INSERT INTO ua.user_events_braze_import
WITH pu_event_new AS (
    SELECT username, 'PrimaryUserEnteredWeek2' AS event_name, week_end AS event_time
    FROM analytics.ua_primary_users_by_week a
    WHERE week_end = '{{ ds }}' AND week_num = 2 AND pu = 1
    UNION ALL SELECT
        username,
        CASE WHEN pu = 1 THEN 'PrimaryUserEntered'
            WHEN pu = 0 THEN 'PrimaryUserExited' END AS event_name,
        week_end AS event_time
    FROM analytics.ua_primary_users_by_week
    WHERE week_end = '{{ ds }}'
),

pixalate_flag_event AS (
    SELECT r.username
    FROM support.pixalate_devices p
    LEFT JOIN adjust.installs_with_pi i ON (p.device_id = i.gps_adid)
    LEFT JOIN adjust.registrations r ON (i.adid = r.adid) AND (i.app_name = r.app_name)
    WHERE
        (id_type = 'ADID')
        AND (i.app_name = 'com.enflick.android.TextNow')
        AND (NVL(r.username, p.device_id) IS NOT NULL)
    UNION ALL SELECT r.username
    FROM support.pixalate_devices p
    LEFT JOIN adjust.installs_with_pi i ON (p.device_id = i.idfa)
    LEFT JOIN adjust.registrations r ON (i.adid = r.adid) AND (i.app_name = r.app_name)
    WHERE
        (id_type = 'IDFA')
        AND (i.app_name IN ('com.tinginteractive.usms', 'com.enflick.ios.textnow'))
        AND (NVL(r.username, i.idfv) IS NOT NULL)
),

new_events AS (
    SELECT DISTINCT username, 'PixalateFlagged' AS event_name, ' {{ts}} ' as event_time
    FROM pixalate_flag_event
    WHERE username IS NOT NULL
    UNION ALL SELECT username,event_name,event_time
    FROM pu_event_new
),

events_imported AS (
    SELECT username, event_name
    FROM ua.user_events_braze_import
    WHERE event_name in ('PrimaryUserEntered', 'PrimaryUserExited', 'PrimaryUserEnteredWeek2','PixalateFlagged')
    QUALIFY DENSE_RANK() OVER ( PARTITION BY username,event_name ORDER BY event_time DESC) = 1
    ORDER BY username
)

SELECT DISTINCT a.username, u.user_id_hex, a.event_name, a.event_time, '{{ ts }}'
FROM new_events a
INNER JOIN prod.core.users u ON u.username = a.username
LEFT JOIN events_imported b ON a.username = b.username AND a.event_name = b.event_name
WHERE b.username IS NULL;
