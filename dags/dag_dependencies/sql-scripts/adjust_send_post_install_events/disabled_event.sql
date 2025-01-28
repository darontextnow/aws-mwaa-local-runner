SELECT DISTINCT  
    CASE a.app_id
        WHEN 'com.enflick.android.TextNow' THEN 'fnz52pd8uj2r'
        WHEN '314716233' THEN 'szvvsxe5b4t2'
    END AS app_token,
    a.app_id,
    a.adid,
    CASE a.app_id
        WHEN 'com.enflick.android.TextNow' THEN 'jqzi3j'
        WHEN '314716233' THEN '435p7n'
    END AS event_token,
    android_id,
    gps_adid,
    idfa,
    idfv,
    ip_address,
    --  following day's run date's end of the day timestamp,so that the send timestamp > first open session time of an event
    EXTRACT(EPOCH_SECONDS FROM DATEADD(ms, -3, '{{ macros.ds_add(ds, 2) }}') ) AS created_at_unix
FROM dau.bad_sets bs
INNER JOIN dau.device_set ds ON bs.set_uuid = ds.set_uuid
INNER JOIN adjust.installs_with_pi a ON a.adid = ds.adid 
WHERE bs.disabled_date = '{{ ds }}'::DATE
    AND app_token IS NOT NULL