{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='surrogate_key',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

WITH adjust_export AS (
    SELECT 
        date_utc AS event_received_date_utc,
        app_id,
        app_name,
        adjust_id,
        client_version,
        installed_at,
        reattributed_at,
        created_at,
        click_time,
        tracker,
        store,
        impression_based,
        is_organic,
        is_untrusted,
        match_type,
        device_type,
        device_name,
        os_name,
        os_version,
        sdk_version,
        region_code,
        country_code,
        country_subdivision,
        city,
        postal_code,
        language,
        ip_address,
        tracking_limited,
        deeplink,
        timezone,
        connection_type,
        idfa,
        idfv,
        gps_adid,
        android_id,
        reattribution_attribution_window_hours,
        inactive_user_definition_hours,
        DECODE(activity_kind, 'reattribution_update', 1, 'reattribution', 2) AS reattribution_priority,
        SUM((activity_kind = 'reattribution')::INT) OVER
           (PARTITION BY app_name, adjust_id ORDER BY created_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS num_reattribution
    FROM {{ ref('adjust_latest_raw_events') }}
    WHERE (activity_kind IN ('reattribution', 'reattribution_update'))
),

adjust_export_deduped AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['adjust_id', 'client_type', 'reattributed_at']) }} AS surrogate_key,
        client_type,
        adjust_export.*
    FROM adjust_export
    JOIN adjust.apps USING (app_name)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY app_name, adjust_id, num_reattribution ORDER BY reattribution_priority ASC, created_at DESC) = 1
),

adjust_callbacks_deduped AS (
    /*
     Note how the surrogate key in callbacks consists of created_at instead of reattributed_at, based on
     data exploration it is found that created_at in the callback matches the reattributed_at in the raw
     csv exports. The created_at in csv exports is more akin to UPDATED_AT since networks can push
     'reattribution_update`s to Adjust.
     */
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['adid', 'client_type', 'created_at']) }} AS surrogate_key,
        client_type,
        NULL AS event_received_date_utc,
        apps.app_id,
        app_name,
        adid AS adjust_id,
        app_version AS client_version,
        installed_at,
        created_at AS reattributed_at,
        created_at,
        click_time,
        CASE WHEN tracker = 'unattr' AND fb_install_referrer_publisher_platform = 'facebook'
        THEN  MD5(app_name || NVL(fb_install_referrer_campaign_name, '') ||  NVL(fb_install_referrer_adgroup_name, '')) 
        ELSE tracker END AS tracker,
        store,
        impression_based,
        network_name IN ('Organic', 'Google Organic Search', 'TextNow.com Referral', 'TextNow.com Refferal') 
            AS is_organic,
        network_name = 'Untrusted Devices' AS is_untrusted,
        match_type,
        device_type,
        device_name,
        os_name,
        os_version,
        sdk_version,
        UPPER(region) AS region_code,
        UPPER(country) AS country_code,
        country_subdivision AS country_subdivision,
        city,
        postal_code,
        language,
        ip_address,
        tracking_limited,
        deeplink,
        REGEXP_REPLACE(timezone, '—', '-') AS timezone, -- replace em dash with en dash, which is basically the minus sign
        connection_type,
        idfa,
        idfv,
        gps_adid,
        android_id,
        reattribution_attribution_window_hours,
        inactive_user_definition_hours
    FROM {{ ref('reattribution_with_pi') }}
    JOIN {{ source('adjust', 'apps') }} USING (app_name)
    WHERE
        (installed_at <> created_at)  -- exclude records backfilled from installs

    {% if is_incremental() %}
        -- we don't have the received timestamp for callbacks, so the next best thing is to look back a big
        -- window and grab those that are missing from the main table
        AND (created_at >= (SELECT MAX(event_received_date_utc) - INTERVAL '120 day' FROM {{ this }}))
        AND (surrogate_key NOT IN (SELECT surrogate_key FROM {{ this }}))
    {% endif %}

    QUALIFY ROW_NUMBER() OVER (PARTITION BY app_name, adid, created_at ORDER BY created_at) = 1 -- ensures there are no duplicates
)

SELECT
    surrogate_key,
    a.event_received_date_utc,
    COALESCE(a.adjust_id, b.adjust_id) AS adjust_id,
    COALESCE(a.app_id, b.app_id) AS app_id,
    COALESCE(a.app_name, b.app_name) AS app_name,
    COALESCE(a.client_type, b.client_type) AS client_type,
    COALESCE(a.client_version, b.client_version) AS client_version,
    COALESCE(a.installed_at, b.installed_at) AS installed_at,
    COALESCE(a.reattributed_at, b.reattributed_at) AS reattributed_at,
    COALESCE(a.created_at, b.created_at) AS created_at,
    COALESCE(a.click_time, b.click_time) AS click_time,
    COALESCE(a.tracker, b.tracker) AS tracker,
    COALESCE(a.store, b.store) AS store,
    COALESCE(a.impression_based, b.impression_based) AS impression_based,
    COALESCE(a.is_organic, b.is_organic) AS is_organic,
    COALESCE(a.is_untrusted, b.is_untrusted) AS is_untrusted,
    COALESCE(a.match_type, b.match_type) AS match_type,
    COALESCE(a.device_type, b.device_type) AS device_type,
    COALESCE(a.device_name, b.device_name) AS device_name,
    COALESCE(a.os_name, b.os_name) AS os_name,
    COALESCE(a.os_version, b.os_version) AS os_version,
    COALESCE(a.sdk_version, b.sdk_version) AS sdk_version,
    COALESCE(a.region_code, b.region_code) AS region_code,
    COALESCE(a.country_code, b.country_code) AS country_code,
    COALESCE(a.country_subdivision, b.country_subdivision) AS country_subdivision,
    COALESCE(a.city, b.city) AS city,
    COALESCE(a.postal_code, b.postal_code) AS postal_code,
    COALESCE(a.language, b.language) AS language,
    COALESCE(a.ip_address, b.ip_address) AS ip_address,
    COALESCE(a.tracking_limited, b.tracking_limited) AS tracking_limited,
    COALESCE(a.deeplink, b.deeplink) AS deeplink,
    COALESCE(a.timezone, b.timezone) AS timezone,
    COALESCE(a.connection_type, b.connection_type) AS connection_type,
    COALESCE(a.reattribution_attribution_window_hours,
             b.reattribution_attribution_window_hours) AS reattribution_attribution_window_hours,
    COALESCE(a.inactive_user_definition_hours,
             b.inactive_user_definition_hours) AS inactive_user_definition_hours,
    COALESCE(a.idfa, b.idfa) AS idfa,
    COALESCE(a.idfv, b.idfv) AS idfv,
    COALESCE(a.gps_adid, b.gps_adid) AS gps_adid,
    COALESCE(a.android_id, b.android_id) AS android_id
FROM adjust_export_deduped a
FULL OUTER JOIN adjust_callbacks_deduped b USING (surrogate_key)