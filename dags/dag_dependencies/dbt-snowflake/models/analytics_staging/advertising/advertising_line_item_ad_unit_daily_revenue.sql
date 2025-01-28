{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        incremental_strategy='delete+insert'
    )
}}

WITH line_item_id_ad_unit_revenue AS(
    SELECT date::DATE             AS date_utc,
           {{ normalized_client_type('platform') }} AS client_type,
           coalesce(custom_3, '') AS line_item_id,
           CASE
               WHEN ad_format ILIKE '%Banner%' THEN 'Banner'
               WHEN ad_format ILIKE '%Conversation End Interstitial%' THEN 'Conversation End Interstitial'
               WHEN ad_format ILIKE '%Call End Interstitial%'
                   AND platform ILIKE '%ANDROID%'
                   THEN 'Call End Interstitial'
               WHEN ad_format ILIKE '%Keyboard Medrect%' OR ad_format ILIKE '%Tablet Medrect%' OR
                    ad_format ILIKE '%Conversation Medrect%' OR ad_format ILIKE '%Medrect%'
                   THEN 'Medium Rectangle'
               WHEN ad_format ILIKE '%Call Screen Native Large%'
                   AND platform ILIKE '%ANDROID%'
                   THEN 'Call Screen Native Large'
               WHEN ad_format ILIKE '%Call Screen Native%' AND platform ILIKE '%ANDROID%'
                   THEN 'Call Screen Native'
               WHEN ad_format ILIKE '%Home In-Stream Native Video%'
                   AND platform ILIKE '%ANDROID%'
                   THEN 'Home In-Stream Native Video'
               WHEN ad_format ILIKE '%Home In-Stream Native%' THEN 'Home In-Stream Native'
               WHEN ad_format ILIKE '%Outgoing Call Native%' THEN 'Outgoing Call Native'
               WHEN ad_format ILIKE '%Pre-Roll%' THEN 'Pre-Roll'
               WHEN ad_format ILIKE '%Rewarded Video S2S%' THEN 'Rewarded Video S2S'
               WHEN ad_format ILIKE '%Call Screen Native Large%'
                   AND platform ILIKE '%TN_IOS_FREE%'
                   THEN '%Call Screen Native%'
               WHEN ad_format ILIKE '%Home In-Stream Native Video%'
                   AND platform ILIKE '%TN_IOS_FREE%'
                   THEN '%Outgoing Call Native%'
               WHEN (ad_format ILIKE '%Call End Interstitial%' OR ad_format ILIKE '%Main Screen Interstitial%') AND
                    platform ILIKE '%TN_IOS_FREE%'
                   THEN 'Interstitial'
               WHEN ad_format ILIKE '%Main Screen Interstitial%' THEN 'Main Screen Interstitial'
               WHEN ad_format ILIKE '%Text In-Stream Native Large%' THEN 'Text In-Stream Native Large'
               WHEN ad_format ILIKE '%Text In-Stream Native%' THEN 'Text In-Stream Native'
               WHEN ad_format ILIKE '%Wde Skyscraper%' OR ad_format ILIKE '%Wide Skyscraper%' THEN 'Skyscraper'
               WHEN ad_format IS NULL THEN 'OTHER'
               ELSE 'OTHER'
               END                AS ad_unit,
           SUM(NVL(revenue, 0))   AS revenue
    FROM {{ ref('report') }}
    WHERE date_utc >='2020-01-01'::DATE

    {% if is_incremental() %}

      AND date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 week'

    {% endif %}

    {% if target.name == 'dev' %}

      AND date::DATE >= CURRENT_DATE - INTERVAL '1 week'

    {% endif %}

    GROUP BY date_utc,
             client_type,
             line_item_id,
             ad_unit
)
SELECT  date_utc,
        client_type,
        line_item_id,
        ad_unit,
        revenue
FROM line_item_id_ad_unit_revenue