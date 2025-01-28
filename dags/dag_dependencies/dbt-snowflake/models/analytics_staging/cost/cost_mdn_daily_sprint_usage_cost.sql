/* Keep this model as table. For incremental to work, we have to refresh whenever
   Sprint billing cycle ends which is difficult
 */

{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}


WITH user_period_cumulative_overage AS (
    SELECT
        mdn,
        period_start,
        period_end,
        date_utc,
        m.pool_name,
        mrc_cost,
        NVL(total_bytes, 0) / 1048576.0 AS mb_usage,
        SUM(NVL(mb_contribution_to_pool, 0)) OVER (
            PARTITION BY mdn, m.pool_name, period_start
        ) AS total_contribution_mb,
        SUM(mb_usage) OVER (
            PARTITION BY mdn, m.pool_name, period_start
            ORDER BY date_utc
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_mb_usage,
        GREATEST(0, cumulative_mb_usage - total_contribution_mb) AS cumulative_overage_mb_usage,
        cumulative_overage_mb_usage * pool_overage_cost_per_mb AS cumulative_overage_mb_cost
    FROM {{ ref('cost_mdn_daily_mrc_cost') }} m
    JOIN {{ ref('cost_monthly_sprint_cost') }} c ON
        (date_utc BETWEEN period_start AND period_end)
        AND (c.pool_name = m.pool_name)
    LEFT JOIN {{ ref('sprint_mdn_daily_data_usage') }} USING (date_utc, mdn)
    WHERE (network = 'Sprint')
)

SELECT
    mdn,
    date_utc,
    pool_name,
    mrc_cost,
    mb_usage,
    cumulative_mb_usage,
    cumulative_overage_mb_cost - NVL(lag(cumulative_overage_mb_cost) OVER (
        PARTITION BY mdn, pool_name, period_start
        ORDER BY date_utc
    ), 0) AS mb_usage_overage_cost
FROM user_period_cumulative_overage
ORDER BY 2
