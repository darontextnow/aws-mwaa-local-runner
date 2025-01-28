/* FIXME:
 1. handle refunds
 2. app store doesn't contain individual transaction at order_id level
*/

{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='created_at',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

-- Adjust doesn't provide Apple's IAP transactions

WITH appstore_sale_prices AS (
    /* There is no way to tell what currency a user has made a purchase in,
       so we just find estimate iap_revenue_usd by estimating the weigth average sales price
       on that date.

       Also sometimes we have special pricing for the first month, which we are also not
       taking into account.

       Since we could not identify whether the transaction type, all transactions from the clients
       are considered as purchase.
     */
    SELECT
        begin_date AS date_utc,
        sku,
        parent_identifier,
        title AS product_title,
        product_type_identifier AS product_type,
        /* Focus on available currency_fx only */
        SUM(customer_price * units * c_fx.fx_usd) / SUM(units) AS iap_gross_revenue_usd, -- revenue paid by customers
        SUM(developer_proceeds * units * p_fx.fx_usd) / SUM(units) AS iap_net_revenue_usd -- revenue that TN can receive after Apple's cut
    FROM {{ source('appstore', 'appstore_sales') }}
    JOIN {{ ref('currency_fx') }} AS c_fx ON c_fx.date_utc = begin_date AND customer_currency = c_fx.currency
    JOIN {{ ref('currency_fx') }} AS p_fx ON p_fx.date_utc = begin_date AND currency_of_proceeds = p_fx.currency
    WHERE
        (parent_identifier <> '')  -- IAPs should have a parent identifier
        AND (customer_price > 0)
        AND (units > 0) -- ignore transactions that do not affect the revenue computation and also refunds for now

    {% if target.name == 'dev' %}
        AND (begin_date >= CURRENT_DATE - INTERVAL '1 week')
    {% endif %}

    {% if is_incremental() %}
        AND (begin_date >= (SELECT MAX(created_at::date) - INTERVAL '1 week' FROM {{ this }}))
    {% endif %}

    GROUP BY 1, 2, 3, 4, 5
),
appstore_iap_transactions AS (
    SELECT 
        b.username, 
        iap.createdat AS created_at,
        'iOS' AS app_name,
        iap.productid AS sku_id,
        iap.transactionid AS transaction_id,
        iap.originaltransactionid AS original_transaction_id
    FROM {{ source('core', 'iap') }} AS iap
    LEFT JOIN {{ ref('analytics_users') }} b ON iap.userid = b.user_id_hex
    WHERE 
        (environment = 'Prod')
        AND (vendor = 'APP_STORE') 
        AND (status IN ('INITIAL_BUY', 'DID_RENEW', 'DID_RECOVER', 'INTERACTIVE_RENEWAL'))
),
approx_iap_revenue_usd AS (
    SELECT
        appstore_iap.username,
        appstore_iap.created_at,
        appstore_iap.app_name,
        appstore_iap.sku_id,
        appstore_iap.transaction_id,
        appstore_iap.original_transaction_id,
        appstore_sale_prices.parent_identifier,
        appstore_sale_prices.product_title,
        appstore_sale_prices.product_type,
        appstore_sale_prices.iap_gross_revenue_usd,
        appstore_sale_prices.iap_net_revenue_usd
    FROM appstore_iap_transactions AS appstore_iap
    LEFT JOIN appstore_sale_prices ON
        (appstore_iap.sku_id = appstore_sale_prices.sku)
        AND (appstore_iap.created_at::DATE = appstore_sale_prices.date_utc)


    {% if target.name == 'dev' %}
        AND (created_at::date >= CURRENT_DATE - INTERVAL '1 week')
    {% endif %}

    {% if is_incremental() %}
        AND (created_at::date >= (SELECT MAX(created_at::date) - INTERVAL '1 week' FROM {{ this }}))
    {% endif %}
)

SELECT
    username,
    COALESCE(mapping.client_type, 'TN_IOS_FREE') AS client_type,
    created_at,
    app_name,
    sku_id,
    transaction_id
    original_transaction_id,
    product_title,
    product_type, -- product_type reported by Apple: https://help.apple.com/app-store-connect/#/dev63c6f4502
    COALESCE(mapping.feature, 'Other') AS product_category,
    mapping.renewal_period AS renewal_period,
    iap_gross_revenue_usd, -- revenue that customer paid
    iap_net_revenue_usd -- revenue that TN can receive after Apple's cut
FROM approx_iap_revenue_usd
LEFT JOIN {{ ref('iap_sku_feature_mapping') }} AS mapping ON (sku_id = mapping.sku)
