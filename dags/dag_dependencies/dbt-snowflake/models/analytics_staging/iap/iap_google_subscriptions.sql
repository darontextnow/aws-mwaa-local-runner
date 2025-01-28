{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

WITH google_iap_transactions AS (
    SELECT  
        a.vendor, 
        a.userid, 
        b.username, 
        a.producttype, 
        a.orderid, 
        a.productid, 
        ifnull(c.feature, 'Other') AS product_category, 
        c.renewal_period, 
        1 AS unit_sold, 
        c.selling_price_usd AS selling_price_usd,
        a.status, 
        a.createdat AS created_at,
        a.expirytime AS expired_at, 
        a.purchasestarttime AS purchase_started_at
    FROM {{ source('core', 'iap') }} a
    LEFT JOIN {{ ref('analytics_users') }} b on a.userid = b.user_id_hex
    LEFT JOIN {{ source('iap', 'iap_sku_feature_mapping') }} c ON (a.productid = c.sku)
    WHERE 
        (a.environment ='Prod') 
        AND (a.producttype = 'IAP_TYPE_SUBSCRIPTION') 
        AND (a.vendor = 'PLAY_STORE')
        AND (a.status IN ('SUBSCRIPTION_PURCHASED', 'SUBSCRIPTION_RENEWED', 'SUBSCRIPTION_RECOVERED'))
)

SELECT  
    vendor, 
    producttype,
    userid, 
    username, 
    orderid,
    CASE WHEN MAX_BY(status, created_at)='SUBSCRIPTION_PURCHASED' THEN 'INITIAL_BUY'
         WHEN MAX_BY(status, created_at) IN ('SUBSCRIPTION_RENEWED', 'SUBSCRIPTION_RECOVERED') THEN 'RENEWAL'
         ELSE MAX_BY(status, created_at) END AS status,
    MIN(created_at::date) AS created_at, 
    MAX(expired_at::date) AS expired_at, 
    MIN(purchase_started_at::date) AS purchase_started_at,
    productid, 
    product_category, 
    renewal_period, 
    MAX(unit_sold) AS unit_sold, 
    MAX(selling_price_usd) AS selling_price_usd
FROM google_iap_transactions
GROUP BY 1, 2, 3, 4, 5, 10, 11, 12
ORDER BY orderid, created_at
