{{
    config(
        tags=['daily'],
        materialized='incremental',
        snowflake_warehouse='PROD_WH_LARGE',
        pre_hook=[
            "/*
            Overwrite existing trackers, insert new trackers. The OR statements are there to reduce the number of rows
            altered, since there will be no need to update a tracker if everything already matches.
            */
            UPDATE {{ this }} tgt
            SET network_name  = s.network_name,
                campaign_name = s.campaign_name,
                adgroup_name  = s.adgroup_name,
                creative_name = s.creative_name
            FROM {{ ref('adjust_trackers_staging') }} s
            WHERE
                (tgt.app_name = s.app_name)
                AND (tgt.tracker = s.tracker)
                AND (
                    (tgt.network_name <> s.network_name)
                    OR (NVL(tgt.campaign_name, '') <> NVL(s.campaign_name, ''))
                    OR (NVL(tgt.adgroup_name,  '') <> NVL(s.adgroup_name, ''))
                    OR (NVL(tgt.creative_name, '') <> NVL(s.creative_name, ''))
                )"
        ]
    )
}}

SELECT s.*
FROM {{ ref('adjust_trackers_staging') }} s
LEFT JOIN {{ this }} t ON (s.app_name = t.app_name) AND (s.tracker = t.tracker)
WHERE (t.tracker IS NULL)
