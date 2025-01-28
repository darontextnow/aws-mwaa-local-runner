/*
 Note that while this model provides monthly cost for TMO, we are limiting our scope to PWG provisioned
 TMO plans because it is the only partner we have to TMO plans at the moment. Assuming we add more
 middle partners down the road, we need to update this model carefully since each partner might have
 different billing cycles.
 */

{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}


WITH pwg_billing_cycle AS (

    -- PWG billing cycle is calendar month
    SELECT
        date_utc,
        DATE_TRUNC('month', date_utc)::DATE AS period_start,
        LAST_DAY(date_utc)::DATE AS period_end
    FROM {{ source('support', 'dates') }}
    WHERE (date_utc BETWEEN '2021-06-01' AND CURRENT_DATE)

),

pwg_user_overage AS (
    -- we calculate the overage at user level first. The true pool-level overage fee
    -- will be spread across the sum of user level overage in each period
    SELECT
        m.mdn,
        m.subscription_rate_plan_id,
        c.period_start,
        c.period_end,
        m.carrier,
        m.pool_name,
        p.per_mb_overage_fee,
        p.per_mou_overage_fee,
        COUNT(1) AS subscriber_days,
        SUM(NVL(m.mb_contribution_to_pool, 0)) AS user_mb_contribution,
        SUM(NVL(d.total_bytes, 0)) / 1048576.0 AS user_mb_usage,
        GREATEST(0, user_mb_usage - user_mb_contribution) AS user_mb_overage,
        SUM(NVL(m.mou_contribution_to_pool, 0)) AS user_mou_contribution,
        SUM(0) AS user_mou_usage,
        GREATEST(0, user_mou_usage - user_mou_contribution) AS user_mou_overage
    FROM {{ ref('cost_mdn_daily_mrc_cost') }} m
    JOIN {{ ref('subscription_rate_plans') }} p ON (m.subscription_rate_plan_id = p.subscription_rate_plan_id)
    JOIN pwg_billing_cycle c ON (m.date_utc = c.date_utc)
    LEFT JOIN {{ ref('tmobile_mdn_daily_data_usage') }} d ON
        (m.date_utc = d.date_utc)
        AND (m.mdn = d.mdn)
        AND (m.carrier = d.carrier)
    WHERE
        (m.network = 'T-Mobile')
        AND (m.carrier = 'PWG')
        AND (m.date_utc < CURRENT_DATE)
        AND (m.date_utc >= COALESCE(plan_start_date, '0001-01-01'))
        AND (m.date_utc <= COALESCE(plan_end_date, '9999-12-31'))
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

),

pwg_pool_overage AS (
    SELECT
        period_start,
        period_end,
        carrier,
        pool_name,
        SUM(subscriber_days) AS subscriber_days,
        -- Data Usage Calc
        SUM(user_mb_contribution) AS pool_mb_limit,
        SUM(user_mb_usage) AS pool_mb_usage,
        GREATEST(0, pool_mb_usage - pool_mb_limit) AS pool_mb_overage,
        MAX(per_mb_overage_fee) * pool_mb_overage AS pool_mb_overage_charge,
        CASE
            WHEN pool_mb_overage > 0
            THEN pool_mb_overage_charge / SUM(user_mb_overage)
            ELSE 0
        END AS pool_overage_cost_per_mb,
        -- MOU Usage Calc
        SUM(user_mou_contribution) AS pool_mou_limit,
        SUM(user_mou_usage) AS pool_mou_usage,
        GREATEST(0, pool_mou_usage - pool_mou_limit) AS pool_mou_overage,
        MAX(per_mou_overage_fee) * pool_mou_overage AS pool_mou_overage_charge,
        CASE
            WHEN pool_mou_overage > 0
            THEN pool_mou_overage_charge / SUM(user_mou_overage)
            ELSE 0
        END AS pool_overage_cost_per_mou
    FROM pwg_user_overage
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM pwg_pool_overage
