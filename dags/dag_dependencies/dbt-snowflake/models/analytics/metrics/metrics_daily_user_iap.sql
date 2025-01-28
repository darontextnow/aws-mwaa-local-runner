{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

WITH iap_apple_subscriptions AS (
    SELECT DISTINCT
        a.createdat::DATE AS date_utc,
        a.createdat AS iap_start_dtm,
        CASE
            WHEN LEAD(status) OVER (PARTITION BY userid, productid ORDER BY createdat) IN ('CANCEL',
                'DID_FAIL_TO_RENEW') THEN LEAD(createdat) OVER (PARTITION BY userid, productid ORDER BY createdat)
            WHEN calculatedexpiredate IS NULL
                THEN LEAD(createdat) OVER (PARTITION BY userid, productid ORDER BY createdat)
            ELSE calculatedexpiredate
        END AS iap_end_dtm,
        a.userid AS user_id_hex,
        'TN_IOS_FREE' AS client_type,
        a.productid AS product_id,
        a.status,
        a.transactionid AS transaction_id,
        a.originaltransactionid AS parent_transaction_id
    FROM (
        SELECT
            MIN(createdat) AS createdat,
            status,
            userid,
            productid,
            calculatedexpiredate,
            producttype,
            transactionid,
            originaltransactionid
        FROM {{ source('core', 'iap') }} a
        WHERE
        {% if is_incremental() or target.name == 'dev' %}
            (createdat BETWEEN {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '1 week' AND {{ var('ds') }}::TIMESTAMP_NTZ)
        {% endif %}
            AND producttype = 'IAP_TYPE_SUBSCRIPTION'
            AND vendor = 'APP_STORE'
            AND environment = 'Prod'
            AND status IN ('INITIAL_BUY', 'DID_RENEW', 'POLL_AUTO_RENEWED',
                'DID_RECOVER', 'INTERACTIVE_RENEWAL', 'CANCEL', 'DID_FAIL_TO_RENEW')
        GROUP BY 2, 3, 4, 5, 6, 7, 8
    ) a
    QUALIFY status NOT IN ('CANCEL', 'DID_FAIL_TO_RENEW') -- use qualify here as it's evaluated after window functions
),
iap_consumables_google_combined AS (
    SELECT DISTINCT
        a.createdat::DATE AS date_utc,
        a.createdat AS iap_start_dtm,
        CASE
            WHEN a.vendor = 'PLAY_STORE' AND a.producttype = 'IAP_TYPE_SUBSCRIPTION' THEN a.expirytime
            WHEN a.vendor = 'PLAY_STORE' AND a.producttype = 'IAP_TYPE_CONSUMABLE' THEN a.expirytime
            ELSE a.createdat
        END AS iap_end_dtm,
        a.userid AS user_id_hex,
        CASE
            WHEN a.vendor = 'APP_STORE' THEN 'TN_IOS_FREE'
            WHEN a.vendor = 'PLAY_STORE' THEN 'ANDROID_INCL_2L'
        END AS client_type,
        a.productid AS product_id,
        a.status,
        CASE
            WHEN a.vendor = 'PLAY_STORE' THEN a.orderid
            ELSE a.transactionid
        END AS transaction_id,
        CASE
            WHEN a.vendor = 'PLAY_STORE' THEN LEFT(a.orderid, 24)
            ELSE a.originaltransactionid
        END AS parent_transaction_id
    FROM {{ source('core', 'iap') }} a
    WHERE
    {% if is_incremental() or target.name == 'dev' %}
        (createdat BETWEEN {{ var('ds') }}::TIMESTAMP_NTZ - INTERVAL '1 week' AND {{ var('ds') }}::TIMESTAMP_NTZ)
    {% endif %}
        AND environment = 'Prod'
        AND ((vendor = 'APP_STORE' AND producttype = 'IAP_TYPE_CONSUMABLE')
            OR (vendor = 'PLAY_STORE' AND producttype = 'IAP_TYPE_SUBSCRIPTION'
                AND status IN ('SUBSCRIPTION_PURCHASED', 'SUBSCRIPTION_RENEWED', 'SUBSCRIPTION_RECOVERED'))
            OR vendor = 'PLAY_STORE' AND producttype = 'IAP_TYPE_CONSUMABLE')
),
iap_all AS (
SELECT * FROM iap_consumables_google_combined
UNION SELECT * FROM iap_apple_subscriptions
)
SELECT
    a.date_utc,
    a.iap_start_dtm,
    a.iap_end_dtm,
    a.user_id_hex,
    u.username,
    u.user_set_id,
    a.client_type,
    a.product_id AS sku_name,
    CASE
        WHEN a.status IN ('INITIAL_BUY', 'SUBSCRIPTION_PURCHASED') THEN 'New Purchase'
        WHEN a.status IN ('DID_RENEW', 'INTERACTIVE_RENEWAL', 'SUBSCRIPTION_RENEWED') THEN 'Renewal'
        WHEN a.status IN ('DID_RECOVER', 'SUBSCRIPTION_RECOVERED') THEN 'Reactivated'
    END AS transaction_status,
    a.transaction_id,
    a.parent_transaction_id,
    f.feature AS sku_category,
    f.selling_price_usd AS sku_selling_price,
    f.renewal_period AS sku_renewal_period
FROM iap_all a
LEFT JOIN {{ ref('iap_sku_feature_mapping') }} f ON a.product_id = f.sku
LEFT JOIN {{ ref('analytics_users') }} u ON a.user_id_hex = u.user_id_hex
QUALIFY ROW_NUMBER() OVER (PARTITION BY iap_end_dtm, a.user_id_hex,
    product_id, CASE WHEN a.client_type = 'ANDROID_INCL_2L' THEN
        LEFT(transaction_id, 24) ELSE transaction_id END ORDER BY iap_start_dtm) = 1