{{
    config(
        tags=['daily'],
        materialized='incremental',
        snowflake_warehouse='PROD_WH_LARGE',
        pre_hook = [
            "--start with fresh copy of installs_with_pi table (can't use ref here due to it would create a cycle)
            CREATE OR REPLACE TABLE {{ this }} CLONE prod.adjust.installs_with_pi COPY GRANTS",
            "--Remove illegitimate installs
            DELETE FROM {{ this }}
            WHERE (environment IS NOT NULL) AND (environment <> 'production')"
        ],
        post_hook = [
            "--STEP 2: After all install events are backfilled (below), update all install records within the last 7 days with the newest attribution information.
            UPDATE {{ this }} tgt
            SET click_time = u.click_time,
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
                twitter_line_item_id = u.twitter_line_item_id,
                fb_install_referrer_campaign_group_name = u.fb_install_referrer_campaign_group_name,
                fb_install_referrer_campaign_group_id = u.fb_install_referrer_campaign_group_id,
                fb_install_referrer_campaign_name = u.fb_install_referrer_campaign_name,
                fb_install_referrer_campaign_id = u.fb_install_referrer_campaign_id,
                fb_install_referrer_adgroup_name = u.fb_install_referrer_adgroup_name,
                fb_install_referrer_adgroup_id = u.fb_install_referrer_adgroup_id,
                fb_install_referrer_ad_id=u.fb_install_referrer_ad_id,
                fb_install_referrer_account_id=u.fb_install_referrer_account_id,
                fb_install_referrer_publisher_platform=u.fb_install_referrer_publisher_platform
            FROM (
                SELECT *
                FROM {{ source('adjust', 'updated_attribution_with_pi') }}
                WHERE
                    (attribution_updated_at >= {{ var('data_interval_start') }}::TIMESTAMP_NTZ - INTERVAL '3 DAYS')
                    AND (activity_kind = 'install_update')
                --use latest updated record when there is more than 1
                QUALIFY ROW_NUMBER() OVER (PARTITION BY adid, app_name ORDER BY created_at DESC) = 1
            ) u
            WHERE
                (tgt.installed_at >= {{ var('data_interval_start') }}::TIMESTAMP_NTZ - INTERVAL '7 DAYS')
                AND (u.adid = tgt.adid)
                AND (u.app_name = tgt.app_name)
                AND (u.attribution_updated_at > tgt.installed_at)
                AND (tgt.tracker = u.outdated_tracker);"
        ]
    )
}}

/*
    STEP 1:
    We need to backfill all missing installation records from 3 sources in the following
    priority list:
      1. adjust.updated_attribution_with_pi
      2. adjust.reattribution_with_pi
      3. adjust.sessions_with_pi
    Lower ranked sources should NOT overwrite records from higher ranked sources. This is
    because higher ranked sources have more data for each installation record. Ideally we
    want to backfill with the most complete substitute.
*/

WITH from_updated AS (  ----INSERT records from adjust.updated_attribution_with_pi
    SELECT DISTINCT
        ua.adid,
        ua.app_id,
        ua.app_version,
        ua.app_name,
        ua.app_name_dashboard,
        ua.environment,
        ua.created_at,
        ua.installed_at,
        ua.click_time,
        ua.tracker,
        ua.tracker_name,
        ua.store,
        ua.impression_based,
        ua.device_type,
        ua.is_organic,
        ua.match_type,
        ua.device_name,
        ua.user_agent,
        ua.os_name,
        ua.network_name,
        ua.campaign_name,
        ua.adgroup_name,
        ua.creative_name,
        ua.os_version,
        ua.sdk_version,
        ua.idfa,
        ua.idfv,
        ua.android_id,
        ua.gps_adid,
        ua.gclid,
        ua.win_naid,
        ua.win_adid,
        ua.ip_address,
        ua.language,
        ua.country,
        ua.fb_campaign_group_name,
        ua.fb_campaign_group_id,
        ua.fb_campaign_name,
        ua.fb_campaign_id,
        ua.fb_adgroup_name,
        ua.fb_adgroup_id,
        ua.tweet_id,
        ua.twitter_line_item_id,
        NULL AS isp,
        NULL AS tracking_limited,
        NULL AS deeplink,
        NULL AS timezone,
        NULL AS connection_type,
        NULL AS rejection_reason,
        ua.app_version_temp,
        ua.app_version_short,
        ua.fb_install_referrer_campaign_group_name,
        ua.fb_install_referrer_campaign_group_id,
        ua.fb_install_referrer_campaign_name,
        ua.fb_install_referrer_campaign_id,
        ua.fb_install_referrer_adgroup_id,
        ua.fb_install_referrer_adgroup_name,
        ua.fb_install_referrer_ad_id,
        ua.fb_install_referrer_account_id,
        ua.fb_install_referrer_publisher_platform,
        CURRENT_TIMESTAMP AS inserted_timestamp,
        NULL AS s3_file_path
    FROM {{ source('adjust', 'updated_attribution_with_pi') }} ua
    LEFT JOIN {{ this }} i USING (adid, app_name, installed_at)
    WHERE
        (ua.created_at >= {{ var('data_interval_start') }}::TIMESTAMP_NTZ - INTERVAL '3 DAYS')
        AND ((ua.environment = 'production') OR (ua.environment IS NULL))
        AND (i.adid IS NULL)
),

-- include distinct records from adjust.reattribution_with_pi
-- We started ingesting Adjust data (Feb 2016) before user acquisition (Jun 2016)
-- If we see an adid FROM reattribution_with_pi but not regular installs, chances are they are organic
from_reattribution AS (
    SELECT DISTINCT
        ar.adid,
        ar.app_id,
        ar.app_version,
        ar.app_name,
        ar.app_name_dashboard,
        ar.environment,
        ar.created_at,
        ar.installed_at,
        NULL::TIMESTAMP_NTZ AS click_time,
        DECODE(ar.app_id,
            'com.enflick.ios.textnow', 'chwusy',
            'com.tinginteractive.usms', 'chwusy',
            'com.enflick.android.TextNow', 'tl1c5q',
            'com.enflick.android.tn2ndLine', '6yw2zl'
        ) AS tracker,
        'Organic' AS tracker_name,
        ar.store,
        FALSE AS impression_based,
        ar.device_type,
        TRUE AS is_organic,
        NULL AS match_type,
        ar.device_name,
        ar.user_agent,
        ar.os_name,
        'Organic' AS network_name,
        NULL AS campaign_name,
        NULL AS adgroup_name,
        NULL AS creative_name,
        ar.os_version,
        ar.sdk_version,
        ar.idfa,
        ar.idfv,
        ar.android_id,
        ar.gps_adid,
        ar.gclid,
        ar.win_naid,
        ar.win_adid,
        ar.ip_address,
        ar.language,
        ar.country,
        NULL AS fb_campaign_group_name,
        NULL AS fb_campaign_group_id,
        NULL AS fb_campaign_name,
        NULL AS fb_campaign_id,
        NULL AS fb_adgroup_name,
        NULL AS fb_adgroup_id,
        NULL AS tweet_id,
        NULL AS twitter_line_item_id,
        NULL AS isp,
        NULL AS tracking_limited,
        NULL AS deeplink,
        NULL AS timezone,
        NULL AS connection_type,
        NULL AS rejection_reason,
        ar.app_version_temp,
        ar.app_version_short,
        ar.fb_install_referrer_campaign_group_name,
        ar.fb_install_referrer_campaign_group_id,
        ar.fb_install_referrer_campaign_name,
        ar.fb_install_referrer_campaign_id,
        ar.fb_install_referrer_adgroup_id,
        ar.fb_install_referrer_adgroup_name,
        ar.fb_install_referrer_ad_id,
        ar.fb_install_referrer_account_id,
        ar.fb_install_referrer_publisher_platform,
        CURRENT_TIMESTAMP AS inserted_timestamp,
        NULL AS s3_file_path
    --Below selects from reattribution table before it gets updated which by models. This is by original design.
    --reattribution_with_pi is mainly populated by snowpipe and then added to and updated by models.
    FROM adjust.reattribution_with_pi ar --cannot use dbt {{ ref }} here as it will cause a cycle
    LEFT JOIN {{ this }} i USING (adid, app_name, installed_at)
    LEFT JOIN from_updated u USING (adid, app_name, installed_at)
    WHERE
        (ar.created_at >= {{ var('data_interval_start') }}::TIMESTAMP_NTZ - INTERVAL '3 DAYS')
        AND ((ar.environment = 'production') OR (ar.environment IS NULL))
        AND (i.adid IS NULL) --don't include rows already in table
        AND (u.adid IS NULL) --don't insert rows already being added by from_updated
),

-- include distinct records from adjust.sessions_with_pi
from_sessions AS (
    SELECT DISTINCT
        s.adid,
        s.app_id,
        s.app_version,
        s.app_name,
        s.app_name_dashboard,
        NULL AS environment,
        s.created_at,
        s.installed_at,
        NULL::TIMESTAMP_NTZ AS click_time,
        s.tracker,
        NULL AS tracker_name,
        s.store,
        NULL AS impression_based,
        NULL AS device_type,
        s.is_organic,
        NULL AS match_type,
        NULL AS device_name,
        NULL AS user_agent,
        NULL AS os_name,
        IFF(s.is_organic, 'Organic', t.network_name) AS network_name,
        NULLIF(t.campaign_name, 'MISSING_CAMPAIGN') AS campaign_name,
        NULLIF(t.adgroup_name, 'MISSING_ADGROUP') AS adgroup_name,
        NULLIF(t.creative_name, 'MISSING_CREATIVE') AS creative_name,
        NULL AS os_version,
        NULL AS sdk_version,
        s.idfa,
        s.idfv,
        s.android_id,
        s.gps_adid,
        NULL AS gclid,
        NULL AS win_naid,
        NULL AS win_adid,
        NULL AS ip_address,
        NULL AS language,
        NULL AS country,
        NULL AS fb_campaign_group_name,
        NULL AS fb_campaign_group_id,
        NULL AS fb_campaign_name,
        NULL AS fb_campaign_id,
        NULL AS fb_adgroup_name,
        NULL AS fb_adgroup_id,
        NULL AS tweet_id,
        NULL AS twitter_line_item_id,
        NULL AS isp,
        NULL AS tracking_limited,
        NULL AS deeplink,
        NULL AS timezone,
        NULL AS connection_type,
        NULL AS rejection_reason,
        s.app_version_temp,
        s.app_version_short,
        NULL AS fb_install_referrer_campaign_group_name,
        NULL AS fb_install_referrer_campaign_group_id,
        NULL AS fb_install_referrer_campaign_name,
        NULL AS fb_install_referrer_campaign_id,
        NULL AS fb_install_referrer_adgroup_id,
        NULL AS fb_install_referrer_adgroup_name,
        NULL AS fb_install_referrer_ad_id,
        NULL AS fb_install_referrer_account_id,
        NULL AS fb_install_referrer_publisher_platform,
        CURRENT_TIMESTAMP AS inserted_timestamp,
        NULL AS s3_file_path
    --Below selects from sessions table before it gets updated which by models. This is by original design.
    --sessions is mainly populated by snowpipe and then added to and updated by models.
    FROM adjust.sessions_with_pi s --cannot use dbt {{ ref }} here as it will cause a cycle
    JOIN {{ ref('trackers') }} t USING (app_name, tracker)
    LEFT JOIN {{ this }} i USING (adid, app_name, installed_at)
    LEFT JOIN from_updated u USING (adid, app_name, installed_at)
    LEFT JOIN from_reattribution r USING (adid, app_name, installed_at)
    WHERE
        (s.created_at >= {{ var('data_interval_start') }}::TIMESTAMP_NTZ - INTERVAl '3 DAYS')
        AND (i.adid IS NULL) --don't include rows already in table
        AND (u.adid IS NULL) --don't insert rows already being added by from_updated
        AND (r.adid IS NULL) --don't insert rows already being added by from_reattribution
    QUALIFY ROW_NUMBER() OVER (PARTITION BY adid, app_id, app_name ORDER BY created_at) = 1 -- only include first session
)

SELECT * FROM from_updated
UNION ALL SELECT * FROM from_reattribution
UNION ALL SELECT * FROM from_sessions
