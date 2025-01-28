/* FIXME:
 1. handle refunds from app store
 2. app store doesn't contain individual transaction at order_id level, so we assume all transactions are charged (new purchase or renewal)
 3. assume the null renewal period to be monthly
*/


{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

WITH iap_google_play AS (
    -- charged timestamp and refunded timestamp (if exists) for each order
    SELECT 
        order_id,
        username,
        sku_id,
        sub_cycle_number,
        product_category,
        renewal_period,
        MIN(CASE WHEN financial_status = 'Charged' THEN created_at ELSE NULL END) AS charged_created_at,
        MAX(CASE WHEN financial_status = 'Refund' THEN created_at ELSE NULL END)  AS refund_created_at
    FROM {{ ref('iap_google_play') }}
    WHERE (created_at < CURRENT_DATE)

    {% if target.name == 'dev' %}
        AND (created_at >= CURRENT_DATE - INTERVAL '1 week')
    {% endif %}

    {% if is_incremental() %}
        AND (created_at >= (SELECT MAX(date_utc) FROM {{ this }}) - INTERVAL '31 days')
    {% endif %}

    GROUP BY 1, 2, 3, 4, 5, 6
    HAVING (charged_created_at IS NOT NULL) -- Ignore the refund for old transaction
),

iap_subscription_history AS (
    -- Assume the null renewal period as monthly
    SELECT DISTINCT      
        username,
        sku_id,
        CASE WHEN renewal_period IS NOT NULL AND renewal_period != '' THEN product_category || ' (' || renewal_period || ')' 
            ELSE product_category END AS feature,
        created_at AS from_ts,
        -- Assume there is no refund from app store
        CASE WHEN renewal_period = 'annually' THEN DATEADD(day, -1, DATEADD(year, 1, created_at))
            WHEN renewal_period = 'monthly' THEN DATEADD(day, -1, DATEADD(month, 1, created_at)) -- monthly
            ELSE DATEADD(day, -1, DATEADD(month, 1, created_at)) -- assume the one-time purchase plan is active for 30 days 
            END AS to_ts
    FROM {{ ref('iap_appstore') }}
    WHERE (iap_net_revenue_usd > 0)

    {% if target.name == 'dev' %}
        AND (created_at >= CURRENT_DATE - INTERVAL '1 week')
    {% endif %}

    {% if is_incremental() %}
        AND (created_at >= (SELECT MAX(date_utc) FROM {{ this }}) - INTERVAL '31 days')
    {% endif %}
 
    UNION SELECT DISTINCT
        username,
        sku_id,
        CASE WHEN renewal_period IS NOT NULL AND renewal_period != '' THEN product_category || ' (' || renewal_period || ')' 
            ELSE product_category END AS feature,
        charged_created_at AS from_ts,
        -- Assume the null renewal period as monthly
        -- Annual plans
        CASE
            WHEN refund_created_at < DATEADD(day, -1, DATEADD(year, 1, charged_created_at)) AND renewal_period = 'annually' -- Refund before subscription ends
            THEN refund_created_at
            WHEN refund_created_at IS NULL AND renewal_period = 'annually' -- annual 
            THEN DATEADD(day, -1, DATEADD(year, 1, charged_created_at)) -- No refund
        -- Monthly plans
            WHEN renewal_period = 'monthly' AND refund_created_at < DATEADD(day, -1, DATEADD(month, 1, charged_created_at)) -- Refund before subscription ends 
            THEN refund_created_at
            WHEN refund_created_at IS NULL AND renewal_period = 'monthly' -- monthly 
            THEN DATEADD(day, -1, DATEADD(month, 1, charged_created_at)) -- No refund
            ELSE DATEADD(day, -1, DATEADD(month, 1, charged_created_at)) -- -- assume the one-time purchase plan is active for 30 days 
            END AS to_ts
    FROM iap_google_play
)

SELECT DISTINCT
    date_utc,
    hist.username,
    hist.sku_id,
    hist.feature
FROM iap_subscription_history AS hist
{{ history_to_daily_join('hist.from_ts', 'hist.to_ts') }}
WHERE (date_utc < CURRENT_DATE)

{% if is_incremental() %}
    AND (date_utc >= (SELECT MAX(date_utc) - INTERVAL '1 week' FROM {{ this }}))
{% endif %}

{% if target.name == 'dev' %}
    AND (date_utc >= CURRENT_DATE - INTERVAL '31 days')
{% endif %}

ORDER BY 1
