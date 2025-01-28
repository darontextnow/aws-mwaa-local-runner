{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}


WITH google_play AS (
    SELECT
        order_ts::date AS date_utc,
        CASE WHEN product_id = 'com.enflick.android.TextNow' THEN 'TN_ANDROID'
            WHEN product_id = 'com.enflick.android.tn2ndLine' THEN '2L_ANDROID'
            ELSE 'Other' END AS client_type,
        country AS country_code,
        sku_id,
        COALESCE(mapping.feature, 'Other') AS product_category,
        mapping.renewal_period AS renewal_period,
        SUM(CASE WHEN financial_status = 'Charged' THEN 1 ELSE 0 END) AS units_sold,
        SUM(CASE WHEN financial_status = 'Refund' THEN 1 ELSE 0 END) AS units_refunded,
        units_sold - units_refunded as net_units_sold,
        SUM(CASE WHEN financial_status = 'Charged' THEN COALESCE(item_price*fx_usd, selling_price_usd) ELSE 0 END) AS sales_revenue,
        SUM(CASE WHEN financial_status = 'Refund' THEN COALESCE(item_price*fx_usd, selling_price_usd) ELSE 0 END) AS refund,
        SUM(CASE WHEN financial_status = 'Refund' AND sku_id in ('5_dollars_international_credit') THEN COALESCE(item_price*fx_usd*0.30, selling_price_usd*0.30) 
                WHEN financial_status = 'Refund' AND sku_id not in ('5_dollars_international_credit') THEN COALESCE(item_price*fx_usd*0.15, selling_price_usd*0.15)
                ELSE 0 END) AS refund_fee,
        SUM(taxes_collected*fx_usd) AS taxes_collected,
        sales_revenue - refund AS gross_revenue,
        CASE WHEN sku_id IN ('5_dollars_international_credit') THEN gross_revenue * 0.7 ELSE gross_revenue * 0.85 END AS net_revenue
    FROM {{ source('google_play', 'google_play_sales') }}
    LEFT JOIN {{ ref('iap_sku_feature_mapping') }} AS mapping ON
        sku_id = mapping.sku
        AND (mapping.client_type = (
            CASE WHEN product_id ILIKE '%com.enflick.android.TextNow%' THEN 'TN_ANDROID'
                 WHEN product_id ILIKE '%com.enflick.android.tn2ndLine%' THEN '2L_ANDROID'
            ELSE 'OTHER' END
        ))
    LEFT JOIN {{ ref('currency_fx') }} fx ON
        (fx.date_utc = order_ts::DATE)
        AND (google_play_sales.sales_currency = fx.currency)
    WHERE
        (order_ts::date < CURRENT_DATE)
        AND (item_price != 0) -- ignore the transactions that do not affect the revenue computation

    {% if is_incremental() %}
        AND (order_ts::date >= (SELECT MAX(date_utc) - INTERVAL '1 week' FROM {{ this }}))
    {% endif %}

    {% if target.name == 'dev' %}
        AND (order_ts::date >= CURRENT_DATE - INTERVAL '1 week')
    {% endif %}

    GROUP BY 1, 2, 3, 4, 5, 6
),

appstore AS (
    SELECT
        begin_date AS date_utc,
        COALESCE(mapping.client_type, 'TN_IOS_FREE') AS client_type,
        country_code,
        sku AS sku_id,
        COALESCE(mapping.feature, 'Other') AS product_category,
        mapping.renewal_period AS renewal_period,
        SUM (CASE WHEN units > 0 THEN units ELSE 0 END) AS units_sold,
        SUM (CASE WHEN units < 0 THEN abs(units) ELSE 0 END) AS units_refunded,
        SUM (units) AS net_units_sold,
        SUM (CASE WHEN units > 0 THEN COALESCE(customer_price * c_fx.fx_usd, selling_price_usd) * units ELSE 0 END) AS sales_revenue,
        SUM (CASE WHEN units < 0 THEN COALESCE(customer_price * c_fx.fx_usd, -selling_price_usd) * units ELSE 0 END) AS refund,
        SUM (CASE WHEN units < 0 THEN COALESCE(customer_price * c_fx.fx_usd * 0.30, -selling_price_usd * 0.30) * units ELSE 0 END) AS refund_fee,
        NULL AS taxes_collected,
        SUM (CASE WHEN units > 0 THEN COALESCE(customer_price * c_fx.fx_usd, selling_price_usd) * units 
                ELSE COALESCE(customer_price * c_fx.fx_usd, -selling_price_usd) * abs(units) END) AS gross_revenue,
        SUM (COALESCE(developer_proceeds * p_fx.fx_usd, developer_proceeds_usd) * units) AS net_revenue
    FROM {{ source('appstore', 'appstore_sales') }}
    LEFT JOIN {{ ref('iap_sku_feature_mapping') }} AS mapping USING(sku)
    LEFT JOIN {{ ref('currency_fx') }} AS c_fx ON c_fx.date_utc = begin_date AND customer_currency = c_fx.currency
    LEFT JOIN {{ ref('currency_fx') }} AS p_fx ON p_fx.date_utc = begin_date AND currency_of_proceeds = p_fx.currency
    WHERE
        (begin_date < CURRENT_DATE)
        AND (parent_identifier <> '')  -- IAPs should have a parent identifier
        AND (developer_proceeds != 0) -- ignore the transactions that do not affect the revenue computation
    
    {% if is_incremental() %}
        AND (begin_date >= (SELECT MAX(date_utc) - INTERVAL '1 week' FROM {{ this }}))
    {% endif %}

    {% if target.name == 'dev' %}
        AND (begin_date >= CURRENT_DATE - INTERVAL '1 week')
    {% endif %}

    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT *
FROM google_play
UNION ALL SELECT * FROM appstore
