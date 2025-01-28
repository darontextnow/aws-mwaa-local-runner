-- depends_on: {{ ref('cost_monthly_sprint_cost') }}, {{ ref('cost_monthly_message_cost') }}

{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

{% if is_incremental() %}
    {% set query %}
        SELECT MAX(period_start) FROM {{ ref('cost_monthly_message_cost') }}
    {% endset %}

    {% if execute %}
        {% set increment_dt = run_query(query).columns[0][0] %}
    {% endif %}
{% endif %}

{% set where_filter %}
    {% if target.name == 'dev' %}
        (date_utc = {{ var('ds') }}::DATE) AND (username IS NOT NULL)
    {% elif is_incremental() %}
        (date_utc >= '{{ increment_dt }}'::DATE) AND (username IS NOT NULL)
    {% else %}
        (date_utc BETWEEN '2020-01-01' AND {{ var('ds') }}::DATE) AND (username IS NOT NULL)
    {% endif %}
{% endset %}

WITH revenue_user_daily_ad AS (
    SELECT 
        date_utc,
        username,
        SUM(NVL(adjusted_revenue, 0)) AS ad_revenue
    FROM {{ ref ('revenue_user_daily_ad') }}
    WHERE {{ where_filter }}
    GROUP BY 1, 2
),

revenue_user_daily_iap AS (
    SELECT date_utc,
        username,
        SUM(CASE WHEN product_category ILIKE 'International Credit%' OR product_category = 'Calling Forward'
              THEN iap_revenue ELSE 0 END) AS iap_credit_revenue,
        SUM(CASE WHEN product_category NOT ILIKE 'International Credit%' AND product_category != 'Calling Forward' AND product_category != 'Other' 
              THEN iap_revenue ELSE 0 END) AS iap_sub_revenue,
        SUM(CASE WHEN product_category = 'Other' 
              THEN iap_revenue ELSE 0 END) AS iap_other_revenue
    FROM {{ ref('revenue_user_daily_iap') }}
    WHERE {{ where_filter }}
    GROUP BY 1, 2
),

revenue_user_daily_purchase AS (
    SELECT 
        date_utc,
        username,
        {{ dbt_utils.pivot(
            'item_category',
            ['subscription', 'credit', 'device_and_plan', 'other'],
            suffix='_revenue',
            then_value='revenue'
        ) }}
    FROM {{ ref('revenue_user_daily_purchase') }}
    WHERE {{ where_filter }}
    GROUP BY 1, 2
),

cost_user_daily_message_cost AS (
    SELECT 
        date_utc,
        username,
        SUM(NVL(total_mms_cost, 0)) AS mms_cost,
        SUM(NVL(total_sms_cost, 0)) AS sms_cost,
        SUM(NVL(total_mms_cost, 0)) + SUM(NVL(total_sms_cost, 0)) AS message_cost
    FROM {{ ref ('cost_user_daily_message_cost')}}
    WHERE {{ where_filter }}
    GROUP BY 1, 2
),

cost_user_daily_call_cost AS (
    SELECT 
        date_utc,
        username,
        SUM(NVL(termination_cost, 0)) + SUM(NVL(layered_paas_cost, 0)) AS calling_cost
    FROM {{ ref ('cost_user_daily_call_cost')}}
    WHERE {{ where_filter }}
    GROUP BY 1, 2
),

cost_user_daily_did_cost AS (
    SELECT 
        date_utc,
        username,
        SUM(NVL(did_cost, 0)) AS did_cost
    FROM (
        SELECT
            date_utc,
            username,
            0.01 / days_in_month * SUM(DATEDIFF(
                SECOND,
                GREATEST(assigned_at, DATE_TRUNC('DAY', date_utc)), -- start of date_utc
                LEAST(COALESCE(unassigned_at, CURRENT_DATE), DATEADD(DAY, 1, DATE_TRUNC('DAY', date_utc))) -- end of date_utc
            )) / 86400 AS did_cost  -- DID cost is 1 cent per month per number
        FROM (
            SELECT username, assigned_at, unassigned_at
            FROM {{ ref('phone_number_history') }}
            WHERE (unassigned_at > '{{ increment_dt }}'::DATE) OR (unassigned_at IS NULL)
        )
        {{ history_to_daily_join('assigned_at', 'unassigned_at') }}  --Note: using ARRAY_GENERATE_RANGE to generate dates is too slow in this query
        WHERE {{ where_filter }}
        GROUP BY 1, 2, days_in_month
    )
    GROUP BY 1, 2
),

cost_user_daily_sprint_cost AS (
    SELECT 
        date_utc,
        username,
        SUM(NVL(mrc_cost, 0)) AS mrc_cost,
        SUM(NVL(mb_usage_overage_cost, 0)) AS overage_cost,
        SUM(NVL(mrc_cost, 0)) + SUM(NVL(mb_usage_overage_cost, 0)) AS sprint_cost
    FROM {{ ref ('cost_user_daily_sprint_cost')}}
    WHERE {{ where_filter }}
    GROUP BY 1, 2
),

cost_user_daily_tmobile_cost AS (
    SELECT 
        date_utc,
        username,
        SUM(NVL(mrc_cost, 0)) AS tmobile_mrc_cost,
        SUM(NVL(mb_usage_overage_cost, 0)) AS tmobile_overage_cost,
        SUM(NVL(mrc_cost, 0)) + SUM(NVL(mb_usage_overage_cost, 0)) AS tmobile_cost
    FROM {{ ref ('cost_user_daily_tmobile_cost')}}
    WHERE {{ where_filter }}
    GROUP BY 1, 2
),

dataplan_sales AS (
    SELECT
        DATE(p.plan_start_date) AS date_utc,
        p.username,
        SUM(p.plan_selling_price) AS paid_data_sales
    FROM {{ source('analytics', 'sim_plans') }} p
    WHERE
        {{ where_filter }}
        AND p.transaction_type <> 'Cancelled Paid Data Sub'
        AND p.plan_suite = 'Paid Data'
    GROUP BY 1, 2
),

iap_sales_data AS (
    SELECT
        date_utc,
        username,
        SUM(iap.sku_selling_price) AS iap_sales
    FROM {{ ref('metrics_daily_user_iap') }} iap
    WHERE {{ where_filter }}
    GROUP BY 1, 2
)

SELECT
    date_utc,
    username,
    NVL(ad_revenue, 0) AS ad_revenue,
    NVL(iap_credit_revenue, 0) AS iap_credit_revenue,
    NVL(iap_sub_revenue, 0) AS iap_sub_revenue,
    NVL(iap_other_revenue, 0) AS iap_other_revenue,
    NVL("subscription_revenue", 0) AS wireless_subscription_revenue,
    NVL("credit_revenue", 0) AS credit_revenue,
    NVL("device_and_plan_revenue", 0) AS device_and_plan_revenue,
    NVL("other_revenue", 0) AS other_purchase_revenue,
    NVL(mms_cost, 0) AS mms_cost,
    NVL(sms_cost, 0) AS sms_cost,
    NVL(message_cost, 0) AS message_cost,
    NVL(calling_cost, 0) AS calling_cost,
    NVL(did_cost, 0) AS did_cost,
    NVL(mrc_cost, 0) AS mrc_cost,
    NVL(overage_cost, 0) AS overage_cost,
    NVL(sprint_cost, 0) AS sprint_cost,
    NVL(tmobile_mrc_cost, 0) AS tmobile_mrc_cost,
    NVL(tmobile_overage_cost, 0) AS tmobile_overage_cost,
    NVL(tmobile_cost, 0) AS tmobile_cost,
    (
        NVL(ad_revenue, 0) + NVL(iap_credit_revenue, 0) + NVL(iap_sub_revenue, 0) + NVL(iap_other_revenue, 0) + 
        NVL("subscription_revenue", 0) + NVL("credit_revenue", 0) + NVL("device_and_plan_revenue", 0) + NVL("other_revenue", 0)
    ) - (
        NVL(message_cost, 0) + NVL(calling_cost, 0) +
        NVL(did_cost, 0) + NVL(sprint_cost, 0) + NVL(tmobile_cost, 0)
    ) AS profit,
    NVL(paid_data_sales, 0) AS paid_data_sales,
    NVL(iap_sales, 0) AS iap_sales
FROM revenue_user_daily_ad
FULL OUTER JOIN revenue_user_daily_iap USING (date_utc, username)
FULL OUTER JOIN revenue_user_daily_purchase USING (date_utc, username)
FULL OUTER JOIN cost_user_daily_message_cost USING (date_utc, username)
FULL OUTER JOIN cost_user_daily_call_cost USING (date_utc, username)
FULL OUTER JOIN cost_user_daily_did_cost USING (date_utc, username)
FULL OUTER JOIN cost_user_daily_sprint_cost USING (date_utc, username)
FULL OUTER JOIN cost_user_daily_tmobile_cost USING (date_utc, username)
FULL OUTER JOIN dataplan_sales USING (date_utc, username)
FULL OUTER JOIN iap_sales_data USING (date_utc, username)
ORDER BY 1, 2
