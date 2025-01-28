{# This model is intended to be a shared staging table for downstream models
so that we only read from the very expensive external table adjust.raw_events once. #}
{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

SELECT
    username,
    date_utc,
    activity_kind,
    app_id,
    app_name,
    adid AS adjust_id,
    -- address iOS 15 rollout reporting issue with app version
    CASE WHEN (date_utc >= '2021-09-20'::DATE) AND (app_id = '314716233') AND (app_version RLIKE '\\d+')
        THEN app_version_short
        ELSE app_version END AS client_version,
    installed_at,
    created_at,
    click_time,
    --tracker for facebook installs are coming in as unattributed,therefore tracker is generated to cater to join between installs table with adjust_tracker
    CASE WHEN (tracker = 'unattr') AND (fb_install_referrer_publisher_platform = 'facebook')
        THEN MD5(app_name || NVL(fb_install_referrer_campaign_name, '') ||  NVL(fb_install_referrer_adgroup_name, ''))
        ELSE tracker END AS tracker,
    store,
    impression_based,
    network_name IN ('Organic', 'Google Organic Search', 'TextNow.com Referral', 'TextNow.com Refferal') AS is_organic,
    network_name = 'Untrusted Devices' AS is_untrusted,
    match_type,
    device_type,
    device_name,
    os_name,
    os_version,
    sdk_version,
    UPPER(region) AS region_code,
    UPPER(country) AS country_code,
    country_subdivision,
    city,
    postal_code,
    language,
    ip_address,
    tracking_limited,
    deeplink,
    REGEXP_REPLACE(timezone, '—', '-') AS timezone, -- replace em dash with en dash, which is basically the minus sign
    connection_type,
    cost_type,
    cost_currency,
    reporting_cost,
    idfa,
    idfv,
    gps_adid,
    android_id,
    event,
    reattributed_at,
    reattribution_attribution_window AS reattribution_attribution_window_hours,
    inactive_user_definition AS inactive_user_definition_hours,
    sku,
    order_id
FROM {{ source('adjust', 'raw_events') }}
WHERE
    --per Ganesan, 3 day look back should be enough as adjust is never delayed this long
    (date_utc > {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
    AND (NVL(environment, 'production') = 'production')
    AND ( -- this and covers all current use cases for downstream models using this staging table.
        (activity_kind IN ('reattribution', 'reattribution_update', 'install', 'install_update'))
        OR (event IN (
            '15size',  -- TN_ANDROID
            'f6wqjf',  -- 2L_ANDROID
            '1msnuu',  -- iOS premium (see jira: IOS-5321)
            '8bqz7s'   -- iOS iLD purchase (see jira: IOS-5321)
        ))
    )
