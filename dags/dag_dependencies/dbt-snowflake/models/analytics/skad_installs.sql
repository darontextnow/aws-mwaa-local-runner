/* installs from SKAD network */
{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

SELECT
  CASE WHEN partner='facebook' THEN uuid_string() else SK_TRANSACTION_ID  END AS transaction_id,
  SK_APP_ID         AS APP_ID,
  'TN_IOS_FREE'     AS client_type ,
  SK_VERSION        AS sk_version,
  SK_TS             AS installed_at,
  tracker           AS tracker,
  partner           AS partner_name,
  sk_network_id     AS network_id,
  network_name      AS network_name,
  sk_campaign_id    AS campaign_id,
  campaign_name     AS campaign_name,
  adgroup_name      AS adgroup_name, 
  creative_name     AS creative_name,
  CASE
    WHEN country IS NULL AND (partner_name = 'TikTok for Business' or partner_name='facebook') THEN 'us'
    WHEN country IS NULL AND (campaign_name LIKE '%US%') THEN 'us'
    WHEN country IS NULL AND (campaign_name LIKE '%CAN%') THEN 'ca'
    ELSE country
  END AS country,
  sk_payload:"source-identifier"::VARCHAR as source_identifier,
  sk_redownload AS is_reinstall,
  CASE
    WHEN sk_fidelity_type = 0 THEN 'view-through'
    WHEN sk_fidelity_type = 1 THEN 'click-through'
  END AS sk_fidelity_type,
  sk_payload:"coarse-conversion-value"::string AS coarse_conversion_value,
  sk_conversion_value AS fine_conversion_value,
  sk_source_app_id AS source_app_id,
  sk_payload:"source-domain"::VARCHAR AS source_domain
FROM {{ source('adjust', 'skad_payloads') }}
WHERE
  zeroifnull(sk_payload:"postback-sequence-index"::int) = 0
  AND
  ( ( sk_transaction_id IS NOT NULL AND sk_conversion_value IS NOT NULL  AND NVL(partner,'') <> 'facebook')
    OR (sk_conversion_value IS  NULL  AND partner='facebook' AND sk_transaction_id IS  NULL)
  )
  


 
