{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

SELECT
    created_at::DATE AS date_utc,
    username,
    client_type,
    product_category,
    renewal_period,
    SUM(NVL(iap_net_revenue_usd, 0)) AS iap_revenue
FROM {{ ref('iap_google_play') }}
WHERE (date_utc < CURRENT_DATE)

{% if is_incremental() %}
  AND (date_utc >= (SELECT MAX(date_utc) FROM {{ this }}) - INTERVAL '1 week')
{% endif %}

{% if target.name == 'dev' %}
  AND (date_utc >= CURRENT_DATE - INTERVAL '1 week')
{% endif %}

GROUP BY 1, 2, 3, 4, 5
HAVING (iap_revenue != 0)

UNION ALL SELECT
    created_at::DATE AS date_utc,
    username,
    client_type,
    product_category,
    renewal_period,
    SUM(NVL(iap_net_revenue_usd, 0)) AS iap_revenue
FROM {{ ref('iap_appstore') }}
WHERE (date_utc < CURRENT_DATE)

{% if is_incremental() %}
  AND (date_utc >= (SELECT MAX(date_utc) FROM {{ this }}) - INTERVAL '1 week')
{% endif %}

{% if target.name == 'dev' %}
  AND (date_utc >= CURRENT_DATE - INTERVAL '1 week')
{% endif %}

GROUP BY 1, 2, 3, 4, 5
HAVING (iap_revenue != 0)