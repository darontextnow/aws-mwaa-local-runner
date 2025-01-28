SELECT DISTINCT
    'fnz52pd8uj2r' AS app_token,
    'com.enflick.android.TextNow' AS app_id,
    b.adid,
    '695ray' AS event_token,
    android_id,
    gps_adid,
    idfa,
    idfv,
    ip_address,
    EXTRACT(EPOCH_SECONDS FROM '{{ ds }}'::DATE + INTERVAL '1 days') AS created_at_unix
FROM analytics.ltv_d1_prediction a
INNER JOIN adjust.installs_with_pi b ON (a.adjust_id = b.adid) AND (a.installed_at = b.installed_at)
WHERE
    (a.installed_at >= (
        SELECT MAX(installed_at)
        FROM analytics.ltv_d1_prediction
        WHERE (installed_at::DATE = '{{ ds }}'::DATE)
    ) - interval '1 day')
    AND (a.installed_at < (
        SELECT MAX(installed_at)
        FROM analytics.ltv_d1_prediction
        WHERE (installed_at::DATE = '{{ ds }}'::DATE)
    ))
    AND (a.proba >= .33)
    AND (b.app_id = 'com.enflick.android.TextNow')
