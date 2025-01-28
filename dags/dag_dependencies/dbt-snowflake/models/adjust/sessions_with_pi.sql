{{
    config(
        tags=['daily'],
        materialized='incremental',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

/*
This table is mainly populated by a snowpipe.
Due to the backfill and update steps in installs_with_pi model and errors by Adjust, it is
possible to get duplicate installs that share the same `adid` and `app_id` but
potentially different `installed_at`, `created_at` etc.
In this case, we keep only the first install event in `adjust.installs_with_pi` (see adjust.installs_with_pi model)
and move remaining dupe organic, non-session install events into `adjust.sessions_with_pi` via the query below.
*/

SELECT installs.* FROM (
    SELECT
        adid,
        app_id,
        app_version,
        created_at,
        installed_at,
        tracker,
        store,
        ip_address,
        NULL AS time_spent,
        NULL AS last_time_spent,
        is_organic,
        idfa,
        idfv,
        android_id,
        gps_adid,
        app_name,
        app_name_dashboard,
        NULL AS username,
        isp,
        tracking_limited,
        timezone,
        connection_type,
        NULL AS deeplink,
        NULL AS country,
        NULL AS os_version,
        app_version_temp,
        app_version_short
    FROM {{ ref('adjust_duplicate_installs_staging') }} i
    WHERE
        (network_name = 'Organic')
        AND (nth_install > 1)
        AND (installed_at = created_at)
        AND (environment IS NOT NULL)
) installs
LEFT JOIN {{ this }} s ON
    (installs.adid = s.adid)
    AND (installs.app_id = s.app_id)
    AND (installs.created_at = s.created_at)
    -- need at least 1 month + lookback to account for lag and incorrect created_at timestamps. Using 3 to be safe.
    AND (s.created_at >= {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '3 MONTHS')
WHERE (s.adid IS NULL)

/*
    Note: Per DS-2788 for period 2024-10-31 to 2024-12-01 we executed the following backfill UPDATE statement
    to fill in a high number of null usernames in session_with_pi table
    due to a bug found in android app versions released during this period.

    UPDATE prod.adjust.sessions_with_pi tgt
    SET username = src.username
    FROM (
        SELECT adid, MAX_BY(username, timestamp) AS username
        FROM prod.core.adtracker
        WHERE (date_utc >= '2024-09-01'::DATE) AND (NULLIF(TRIM(username), '') IS NOT NULL)
        GROUP BY 1
    ) src
    WHERE
        (tgt.created_at BETWEEN '2024-10-31'::TIMESTAMP_NTZ AND '2024-12-01'::TIMESTAMP_NTZ)
        AND (tgt.adid = src.adid)
        AND (tgt.app_version >= '24.43.0')
        AND (tgt.username IS NULL)
*/