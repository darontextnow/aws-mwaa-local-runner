{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH iap_revenue AS (
  SELECT 
    date_utc,
    client_type,
    SUM(CASE WHEN product_category ILIKE 'International Credit%' OR product_category = 'Calling Forward'
          THEN net_revenue ELSE 0 END) AS iap_credit_revenue,
    SUM(CASE WHEN product_category NOT ILIKE 'International Credit%' AND product_category != 'Calling Forward' AND product_category != 'Other' 
          THEN net_revenue ELSE 0 END) AS iap_sub_revenue,
    SUM(CASE WHEN product_category = 'Other' 
          THEN net_revenue ELSE 0 END) AS iap_other_revenue
  FROM {{ ref('metrics_daily_iap_revenue') }}
  WHERE (date_utc < CURRENT_DATE)
  
  {% if is_incremental() %}
    AND (date_utc >= (SELECT MAX(date_utc) - INTERVAL '1 week' FROM {{ this }}))
  {% endif %}

  {% if target.name == 'dev' %}
    AND (date_utc >= CURRENT_DATE - INTERVAL '1 week')
  {% endif %}

  GROUP BY 1, 2
),
message_call_metrics AS (
    SELECT
        a.date_utc,
        b.client_type,
        SUM(total_outbound_calls) AS total_outbound_calls,
        SUM(total_inbound_calls) AS total_inbound_calls,
        SUM(total_outbound_messages) AS total_outbound_messages,
        SUM(total_inbound_messages) AS total_inbound_messages
    FROM {{ ref ('metrics_daily_user_summary') }} a
    JOIN {{ ref('dau_user_set_active_days') }} b ON (a.user_set_id = b.user_set_id) AND (a.date_utc = b.date_utc)
    WHERE
    {% if is_incremental() %}
        a.date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 week'
    {% endif %}

    {% if target.name == 'dev' %}
        a.date_utc >= CURRENT_DATE - INTERVAL '1 week'
    {% endif %}
        AND a.user_set_id NOT IN (SELECT set_uuid FROM {{ ref('bad_sets') }})
        GROUP BY 1, 2
)
SELECT
    date_utc,
    client_type,
    dau,
    new_dau,
    d1_return_dau,
    d7_return_dau,
    d14_return_dau,
    d30_return_dau,
    nvl(ad.ad_revenue, 0) AS ad_revenue,
    nvl(iap.iap_credit_revenue, 0) AS iap_credit_revenue,
    nvl(iap.iap_sub_revenue, 0) AS iap_sub_revenue,
    nvl(iap.iap_other_revenue, 0) AS iap_other_revenue,
    nvl(p."subscription_revenue", 0) AS wireless_subscription_revenue,
    nvl(p."device_and_plan_revenue", 0) AS device_and_plan_revenue,
    nvl(p."other_revenue", 0) AS device_and_other_purchase_revenue,
    -- summary columnsF
    nvl(ad.ad_revenue, 0)
      + nvl(iap.iap_credit_revenue, 0)
      + nvl(iap.iap_sub_revenue, 0)
      + nvl(iap.iap_other_revenue, 0)
    AS free_app_net_revenue,
    nvl(p."subscription_revenue", 0)
      + nvl(p."device_and_plan_revenue", 0)
      + nvl(p."other_revenue", 0)
      + nvl(p."credit_revenue", 0)
    AS wireless_net_revenue,

    --calling metrics
    cm.total_outbound_calls,
    cm.total_inbound_calls,

    --message metrics
    cm.total_outbound_messages,
    cm.total_inbound_messages,
    CURRENT_TIMESTAMP AS inserted_at,
    'dbt' AS inserted_by
FROM {{ ref('metrics_daily_active_users') }}
FULL OUTER JOIN {{ ref('metrics_daily_ad_revenue') }} ad USING (date_utc, client_type)
FULL OUTER JOIN iap_revenue iap USING (date_utc, client_type)
FULL OUTER JOIN {{ ref('metrics_daily_purchase_revenue') }} p USING (date_utc, client_type)
FULL OUTER JOIN message_call_metrics cm USING (date_utc, client_type)
WHERE (date_utc < CURRENT_DATE)

{% if is_incremental() %}
  AND (date_utc >= (SELECT MAX(date_utc) - INTERVAL '1 week' FROM {{ this }}))
{% endif %}

{% if target.name == 'dev' %}
  AND (date_utc>= CURRENT_DATE - INTERVAL '1 week')
{% endif %}