{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

SELECT
    date_utc,
    "client_details.client_data.user_data.username" AS username,
    {{ pp_normalized_client_type() }} AS client_type,
    COALESCE("payload.ad_event_data.ad_unit_id", '') AS line_item_id,
    ---Added InHouse logic to capture in house ads as an ad unit category
    CASE
        WHEN "payload.ad_event_data.network" = 'AD_NETWORK_IN_HOUSE' THEN 'IN-HOUSE'
        WHEN "payload.ad_event_data.request_type" = 'AD_TYPE_BANNER' THEN 'Banner'
        WHEN "payload.ad_event_data.request_type" = 'AD_TYPE_INTERSTITIAL' THEN 'Interstitial'
        WHEN "payload.ad_event_data.request_type" = 'AD_TYPE_VIDEO' THEN 'Video'
        WHEN "payload.ad_event_data.request_type" = 'AD_TYPE_NATIVE' THEN 'Native'
        WHEN "payload.ad_event_data.request_type" = 'AD_TYPE_NATIVE_ADVANCED' THEN 'NativeAdvanced'
        ELSE 'OTHER'
    END AS ad_unit,
    NVL(COUNT(1), 0) AS impressions
FROM {{ ref('adshoweffective_combined_data') }}
WHERE (username <> '')

{% if is_incremental() %}
  AND (date_utc BETWEEN (SELECT MAX(date_utc) - INTERVAL '1 week' FROM {{ this }})  AND CURRENT_DATE)
{% else %}
  FAIL NON INCREMENTAL --the adshoweffective_combined_data view only has one month data in it
  -- If need to restate this table, will need to revise query.
{% endif %}

{% if target.name == 'dev' %}
  AND (date_utc BETWEEN CURRENT_DATE - INTERVAL '1 week' AND CURRENT_DATE)
{% endif %}

GROUP BY 1, 2, 3, 4, 5
