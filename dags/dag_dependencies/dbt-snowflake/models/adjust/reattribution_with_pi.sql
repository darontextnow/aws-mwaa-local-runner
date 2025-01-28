{{
    config(
        tags=['daily'],
        materialized='incremental',
        snowflake_warehouse='PROD_WH_LARGE',
        post_hook=[
            "--This table is mainly populated by a snowpipe. UPDATE existing records with latest updated records
            UPDATE {{ this }} tgt SET
                click_time = u.click_time,
                tracker = u.tracker,
                tracker_name = u.tracker_name,
                impression_based = u.impression_based,
                is_organic = u.is_organic,
                match_type = u.match_type,
                network_name = u.network_name,
                campaign_name = u.campaign_name,
                adgroup_name = u.adgroup_name,
                creative_name = u.creative_name,
                gclid = u.gclid,
                fb_campaign_group_name = u.fb_campaign_group_name,
                fb_campaign_group_id = u.fb_campaign_group_id,
                fb_campaign_name = u.fb_campaign_name,
                fb_campaign_id = u.fb_campaign_id,
                fb_adgroup_name = u.fb_adgroup_name,
                fb_adgroup_id = u.fb_adgroup_id,
                tweet_id = u.tweet_id,
                twitter_line_item_id = u.twitter_line_item_id
            FROM {{ source('adjust', 'updated_attribution_with_pi') }} u
            WHERE
                (tgt.created_at >= {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '7 DAYS')
                AND (u.attribution_updated_at >= {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '3 DAYS')
                AND (u.activity_kind = 'reattribution_update')
                AND (u.adid = tgt.adid)
                AND (u.app_name = tgt.app_name)
                AND (u.attribution_updated_at > tgt.created_at)
                AND (tgt.tracker = u.outdated_tracker)"
        ]
    )
}}

/*
    Due to the backfill and update steps in installs_with_pi model and errors by Adjust, it is
    possible to get duplicate installs that share the same `adid` and `app_id` but
    potentially different `installed_at`, `created_at` etc.
    In this case, we keep only the first install event in `adjust.installs_with_pi` (see adjust.installs_with_pi model)
    and move remaining non-organic non-first install events to adjust.reattribution_with_pi via the query below
    using the following criteria:
        Only subsequent non-organic & non-session backfilled installation records can be
         injected into `adjust.reattribution_with_pi`. There are two key points here:
            I. Organic records will never be considered reattributions.
           II. Session records are for installation record backfill purposes only. Only
               legitimate subsequent installation records (not backfilled from session records)
               can be considered reattributions.

    Records inserted by us will have the same created_at AS installed_at
*/
SELECT installs.*
FROM (
    SELECT
        adid,
        app_id,
        app_version,
        app_name,
        app_name_dashboard,
        environment,
        created_at,
        installed_at,
        click_time,
        tracker,
        tracker_name,
        store,
        impression_based,
        device_type,
        is_organic,
        match_type,
        device_name,
        user_agent,
        os_name,
        network_name,
        campaign_name,
        adgroup_name,
        creative_name,
        os_version,
        sdk_version,
        idfa,
        idfv,
        android_id,
        gps_adid,
        gclid,
        win_naid,
        win_adid,
        ip_address,
        language,
        country,
        fb_campaign_group_name,
        fb_campaign_group_id,
        fb_campaign_name,
        fb_campaign_id,
        fb_adgroup_name,
        fb_adgroup_id,
        tweet_id,
        twitter_line_item_id,
        NULL AS reattribution_attribution_window_hours,
        NULL AS inactive_user_definition_hours,
        NULL AS region,
        NULL AS country_subdivision,
        NULL AS city,
        NULL AS postal_code,
        deeplink,
        timezone,
        connection_type,
        tracking_limited,
        app_version_temp,
        app_version_short,
        fb_install_referrer_campaign_group_name,
        fb_install_referrer_campaign_group_id,
        fb_install_referrer_campaign_name,
        fb_install_referrer_campaign_id,
        fb_install_referrer_adgroup_id,
        fb_install_referrer_adgroup_name,
        fb_install_referrer_ad_id,
        fb_install_referrer_account_id,
        fb_install_referrer_publisher_platform,
        CURRENT_TIMESTAMP AS inserted_timestamp,
        NULL AS s3_file_path
    FROM {{ ref('adjust_duplicate_installs_staging') }}
    WHERE
        (network_name <> 'Organic')
        AND nth_install > 1
        AND (installed_at = created_at)
        AND (environment IS NOT NULL)
) installs
LEFT JOIN {{ this }} r ON
    (installs.adid = r.adid)
    AND (installs.app_id = r.app_id)
    AND (installs.created_at = r.created_at)
    AND (r.created_at >= (SELECT MIN(created_at) FROM {{ ref('adjust_duplicate_installs_staging') }}))
WHERE (r.adid IS NULL)
