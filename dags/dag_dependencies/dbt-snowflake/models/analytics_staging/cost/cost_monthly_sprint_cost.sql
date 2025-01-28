/* FIX ME
   MOU Usage is not yet taken into account, pending calls table
*/

{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

WITH sprint_billing_cycle AS (
    -- Sprint billing cycle is 14th of a month to 13th of next month
    SELECT
        date_utc,
        CASE
            WHEN DATE_PART(DAY, date_utc) >= 14
            THEN DATEADD(DAY, 13, DATE_TRUNC('MONTH', date_utc))
            ELSE DATEADD(DAY, 13, ADD_MONTHS(DATE_TRUNC('MONTH', date_utc), -1))
        END::DATE AS period_start,
        DATEADD(DAY, -1, ADD_MONTHS(period_start, 1))::DATE AS period_end
    FROM {{ source('support', 'dates') }}
    WHERE (date_utc BETWEEN '2019-01-14' AND CURRENT_DATE)
),

user_overage AS (
    -- we calculate the overage at user level first. The true pool-level overage fee
    -- will be spread across the sum of user level overage in each period
    SELECT
        mdn,
        subscription_rate_plan_id,
        period_start,
        period_end,
        pool_name,
        per_mb_overage_fee,
        per_mou_overage_fee,
        COUNT(1) AS subscriber_days,
        SUM(NVL(mb_contribution_to_pool, 0)) AS user_mb_contribution,
        SUM(NVL(total_bytes, 0)) / 1048576.0 AS user_mb_usage,
        GREATEST(0, user_mb_usage - user_mb_contribution) AS user_mb_overage,
        SUM(NVL(mou_contribution_to_pool, 0)) AS user_mou_contribution,
        SUM(0) AS user_mou_usage,
        GREATEST(0, user_mou_usage - user_mou_contribution) AS user_mou_overage
    FROM {{ ref('cost_mdn_daily_mrc_cost') }} mrc
    JOIN {{ ref('subscription_rate_plans') }} USING (subscription_rate_plan_id)
    JOIN sprint_billing_cycle c USING (date_utc)
    LEFT JOIN {{ ref('sprint_mdn_daily_data_usage') }} USING (date_utc, mdn)
    WHERE
        (mrc.network = 'Sprint')
        AND (date_utc < CURRENT_DATE)
        AND (date_utc >= COALESCE(plan_start_date, '0001-01-01'))
        AND (date_utc <= COALESCE(plan_end_date, '9999-12-31'))
    GROUP BY 1, 2, 3, 4, 5, 6, 7

),

pool_overage AS (
    SELECT
        period_start,
        period_end,
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
    FROM user_overage
    GROUP BY period_start, period_end, pool_name
)

SELECT * FROM pool_overage
