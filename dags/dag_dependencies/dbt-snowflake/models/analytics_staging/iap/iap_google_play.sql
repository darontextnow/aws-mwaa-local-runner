{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='created_at',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}


WITH username_info AS (
    SELECT DISTINCT iap.orderid AS order_id, b.username
    FROM {{ source('core', 'iap') }} iap
    LEFT JOIN {{ ref('analytics_users') }} b ON (iap.userid = b.user_id_hex)
    WHERE 
        (environment = 'Prod')
        AND (vendor = 'PLAY_STORE') 
        AND (status IN ('SUBSCRIPTION_PURCHASED', 'SUBSCRIPTION_RENEWED', 'SUBSCRIPTION_RECOVERED', 'ONE_TIME_PRODUCT_PURCHASED'))

    {% if target.name == 'dev' %}
        AND (created_at::DATE >= CURRENT_DATE - INTERVAL '1 week')
    {% endif %}

)

/* iap_gross_revenue_usd and iap_net_revenue_usd could be empty, because currency_fx probably doesn't contain all currencies on all dates */

SELECT
    order_ts AS created_at,
    CASE WHEN product_id ILIKE '%com.enflick.android.TextNow%' THEN 'TN_ANDROID'
        WHEN product_id ILIKE '%com.enflick.android.tn2ndLine%' THEN '2L_ANDROID'
        ELSE 'OTHER' END AS client_type,
    username,
    sku_id,
    order_id,
    -- Info from Google's API
    financial_status,
    device_model,
    sub_cycle_number,
    product_title,
    product_type,
    COALESCE(mapping.feature, 'Other') AS product_category,
    mapping.renewal_period AS renewal_period,
    sales_currency,
    item_price,
    taxes_collected,
    charged_amount,
    city,
    state,
    postal_code,
    country,
    currency,
    charged_amount * fx_usd as iap_gross_revenue_usd, -- revenue paid by customers
    CASE WHEN financial_status = 'Charged' THEN item_price * fx_usd * 0.7 
        WHEN financial_status = 'Refund' THEN -1 * item_price * fx_usd * 0.7 END AS iap_net_revenue_usd -- revenue that TN can receive after the Google's cut (30%)
FROM {{ source('google_play', 'google_play_sales') }}
LEFT JOIN {{ ref('iap_sku_feature_mapping') }} AS mapping ON
    (sku_id = mapping.sku)
    AND (mapping.client_type =
        CASE WHEN product_id ILIKE '%com.enflick.android.TextNow%' THEN 'TN_ANDROID'
        WHEN product_id ILIKE '%com.enflick.android.tn2ndLine%' THEN '2L_ANDROID'
        ELSE 'OTHER' END
    )
LEFT JOIN {{ ref('currency_fx') }} fx ON
    (fx.date_utc = order_ts::DATE)
    AND (google_play_sales.sales_currency = fx.currency)
JOIN username_info USING (order_id) -- ignore the NULL usernames
WHERE (item_price != 0) -- ignore the transactions that do not affect the revenue computation

{% if target.name == 'dev' %}
    AND (created_at >= CURRENT_DATE - INTERVAL '1 week')
{% endif %}

{% if is_incremental() %}
    AND (created_at >= (SELECT MAX(created_at) - INTERVAL '1 week' FROM {{ this }}))
{% endif %}
