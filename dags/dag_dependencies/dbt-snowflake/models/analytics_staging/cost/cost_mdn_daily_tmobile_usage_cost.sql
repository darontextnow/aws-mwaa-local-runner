/* Keep this model as table. For incremental to work, we have to refresh whenever
   Sprint billing cycle ends which is difficult
 */

{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}


WITH user_period_cumulative_overage AS (
    SELECT
        m.mdn,
        c.period_start,
        c.period_end,
        m.date_utc,
        m.carrier,
        m.pool_name,
        m.mrc_cost,
        NVL(d.total_bytes, 0) / 1048576.0 AS mb_usage,
        SUM(NVL(m.mb_contribution_to_pool, 0)) OVER (
            PARTITION BY m.mdn, m.carrier, m.pool_name, c.period_start
        ) AS total_contribution_mb,
        SUM(mb_usage) OVER (
            PARTITION BY m.mdn, m.carrier, m.pool_name, c.period_start
            ORDER BY m.date_utc
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_mb_usage,
        GREATEST(0, cumulative_mb_usage - total_contribution_mb) AS cumulative_overage_mb_usage,
        cumulative_overage_mb_usage * c.pool_overage_cost_per_mb AS cumulative_overage_mb_cost
    FROM {{ ref('cost_mdn_daily_mrc_cost') }} m
    JOIN {{ ref('cost_monthly_tmobile_cost') }} c ON
        (m.date_utc BETWEEN c.period_start AND c.period_end)
        AND (m.carrier = c.carrier)
        AND (m.pool_name = c.pool_name)
    LEFT JOIN {{ ref('tmobile_mdn_daily_data_usage') }} d ON
        (m.date_utc = d.date_utc)
        AND (m.mdn = d.mdn)
        AND (m.carrier = d.carrier)
    WHERE (m.network = 'T-Mobile')
)

SELECT
    mdn,
    date_utc,
    carrier,
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
