{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_LARGE',
        post_hook=[
            "/*
            This query creates custom trackers of length 12 that helps us keep track of wasted ad spend.
            It is done last because trackers come from multiple sources, and may need extra processing depending
            on where they came from. It is simply easier to create custom trackers after all the work is done.
            Each tracker for wasted spend will be created at the campaign level, so we will not be creating a lot of them.
            The length of trackers coming from Adjust have been increasing. To avoid unnecessary
            conflict down the line, we prepend 'c_' to each custom tracker of length 10 (10+2 gives 12).
            To identify custom trackers, simply check for trackers like 'c_%'.
            Filtering out Organic and Untrusted Devices because there should be no spend associated with them.
            */
            INSERT INTO {{ this }}
            SELECT DISTINCT
                app_name,
                'c_' || LEFT(MD5(app_name || network_name || campaign_name), 10) AS tracker,
                network_name,
                IFF(TRIM(NVL(campaign_name, '')) = '', 'MISSING_CAMPAIGN', campaign_name) AS campaign_name,
                NULL AS adgroup_name,
                NULL AS creative_name,
                NULL AS fb_campaign_group_id,
                NULL AS fb_campaign_id,
                NULL AS fb_adgroup_id
            FROM {{ this }}
            WHERE (network_name NOT IN ('Organic', 'Untrusted Devices', 'Unattributed', 'TextNow.com Referral'));"
        ]
    )
}}

/*
NOTES:
Trackers coming through `adjust.installs` and `adjust.reattribution` needs extra massaging because they are real time
callbacks. This means one tracker can be associated with two different sets of network, campaign, adgroup and creative.
In this case, we take the latest set. This is similar to an update, except it's done within the same lookback window.

Note that the join key is `app_name` and `tracker`. Since Adjust treat different apps as separate entities,
it cannot guarantee universal tracker uniqueness across different apps.
*/

SELECT
    app_name,
    tracker,
    network_name,
    CASE WHEN network_name NOT IN ('Organic', 'Untrusted Devices', 'Unattributed', 'TextNow.com Referral')
        AND (campaign_name IS NULL OR adgroup_name IS NULL OR creative_name IS NULL)
        AND TRIM(NVL(campaign_name, '')) = '' THEN 'MISSING_CAMPAIGN' ELSE campaign_name END AS campaign_name,
    CASE WHEN network_name NOT IN ('Organic', 'Untrusted Devices', 'Unattributed', 'TextNow.com Referral')
        AND (campaign_name IS NULL OR adgroup_name IS NULL OR creative_name IS NULL)
        AND TRIM(NVL(adgroup_name,  '')) = '' THEN 'MISSING_ADGROUP' ELSE adgroup_name END AS adgroup_name,
    CASE WHEN network_name NOT IN ('Organic', 'Untrusted Devices', 'Unattributed', 'TextNow.com Referral')
        AND (campaign_name IS NULL OR adgroup_name IS NULL OR creative_name IS NULL)
        AND TRIM(NVL(creative_name, '')) = '' THEN 'MISSING_CREATIVE' ELSE creative_name END AS creative_name,
    fb_campaign_group_id,
    fb_campaign_id,
    fb_adgroup_id
FROM (
    WITH install_callback_trackers AS (
        SELECT
            i.app_name,
            i.tracker,
            network_name,
            LAST_VALUE(COALESCE(fb_campaign_group_name, campaign_name) IGNORE NULLS) OVER
                (PARTITION BY tracker ORDER BY installed_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            AS campaign_name,
            LAST_VALUE(COALESCE(fb_campaign_name, adgroup_name) IGNORE NULLS) OVER
                (PARTITION BY tracker ORDER BY installed_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            AS adgroup_name,
            LAST_VALUE(COALESCE(fb_adgroup_name, creative_name) IGNORE NULLS) OVER
                (PARTITION BY tracker ORDER BY installed_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            AS creative_name,
            LAST_VALUE(fb_campaign_group_id IGNORE NULLS) OVER
                (PARTITION BY tracker ORDER BY installed_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            AS fb_campaign_group_id,
            LAST_VALUE(fb_campaign_id IGNORE NULLS) OVER
                (PARTITION BY tracker ORDER BY installed_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            AS fb_campaign_id,
            LAST_VALUE(fb_adgroup_id IGNORE NULLS) OVER
                (PARTITION BY tracker ORDER BY installed_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            AS fb_adgroup_id
        FROM {{ source('adjust', 'installs') }} i
        JOIN {{ source('adjust', 'apps') }} USING (app_name) -- helps filter out invalid app_id/app_names
        WHERE
            (installed_at >= {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '2 DAYS')
            AND (installed_at < {{ var('data_interval_end') }}::DATE::TIMESTAMP_NTZ)
            AND (TRIM(NVL(network_name, '')) <> '')
            AND (i.tracker IS NOT NULL)
            --Facebook installs are coming in as unattr. Eliminating these here as are getting added as part of adjust_trackers model
            AND (i.tracker <> 'unattr')
    ),
    reattribution_callback_trackers AS (
        SELECT
            r.app_name,
            r.tracker,
            network_name,
            LAST_VALUE(COALESCE(fb_campaign_group_name, campaign_name) IGNORE NULLS) OVER
                (PARTITION BY tracker ORDER BY created_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            AS campaign_name,
            LAST_VALUE(COALESCE(fb_campaign_name, adgroup_name) IGNORE NULLS) OVER
                (PARTITION BY tracker ORDER BY created_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            AS adgroup_name,
            LAST_VALUE(COALESCE(fb_adgroup_name, creative_name) IGNORE NULLS) OVER
                (PARTITION BY tracker ORDER BY created_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            AS creative_name,
            LAST_VALUE(fb_campaign_group_id IGNORE NULLS) OVER
                (PARTITION BY tracker ORDER BY created_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            AS fb_campaign_group_id,
            LAST_VALUE(fb_campaign_id IGNORE NULLS) OVER
                (PARTITION BY tracker ORDER BY created_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            AS fb_campaign_id,
            LAST_VALUE(fb_adgroup_id IGNORE NULLS) OVER
                (PARTITION BY tracker ORDER BY created_at ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            AS fb_adgroup_id
        FROM {{ source('adjust', 'reattribution') }} r
        JOIN {{ source('adjust', 'apps') }} USING (app_name) -- helps filter out invalid app_id/app_names
        LEFT JOIN install_callback_trackers i USING (app_name, tracker)
        WHERE
            (created_at >= {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '2 DAYS')
            AND (created_at < {{ var('data_interval_end') }}::DATE::TIMESTAMP_NTZ)
            AND (TRIM(NVL(network_name, '')) <> '')
            AND (r.tracker IS NOT NULL)
            --Facebook installs are coming in as unattr. Eliminating these here as are getting added as part of adjust_trackers model
            AND (r.tracker <> 'unattr')
            AND (i.tracker IS NULL) -- don't include records being inserted by install_callback_trackers
    )
    SELECT * FROM install_callback_trackers
    UNION ALL SELECT * FROM reattribution_callback_trackers
)
