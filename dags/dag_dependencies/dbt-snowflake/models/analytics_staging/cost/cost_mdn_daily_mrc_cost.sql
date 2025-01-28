{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

/*
    The daily MRC calculated here are akin to "accrual basis", attributing the expense
    to the actual dates on which each subscribers were activated; it is different
    from the actual billing ("cash basis") in that:
    1. Sprint "pre-bills" active subs at the beginning of a billing period. They
       are charged the full amount for the billing period; should there be any deactivation
       or change of plans in the middle of the period, pro-rated refunds / adjustments are
       made at the next bill.
    2. For Kore there is no pro-rated discount if it is suspended partway through the billing
       cycle. A device is which active at the beginning of the billing period is charged the
       full amount, and contributes the full amount of its plan to the data pool size. **However**
       here we simplified the calculation for Kore so that we model them like Sprint.
    3. TMO uses pretty much the same setup as Sprint, with some minor differences in how data is
       tracked. As the time of this writing, we go through a middleman (PWG) to get carrier
       subscriptions with TMO. This is why we will add `carrier` as a column to help us
       differentiate (potentially) multiple carriers that supply TMO subscriptions. Also, since
       TMO tracks through carrier subscriptions service (refer to backend) instead of inventory
       versions like we do for Sprint and Kore, we will union TMO data with legacy data here
       to encourage consistency.
 */

SELECT
    m.mdn,
    m.date_utc,
    'T-Mobile' AS network,
    m.carrier AS carrier,
    -- TMO uses mostly the same set-up as sprint
    'Pool' || TO_CHAR(p.mrc, '90D99') AS pool_name,
    m.inventory_uuid AS item_uuid,
    m.iccid,
    m.imsi,
    p.subscription_rate_plan_id,
    p.code,
    CASE
       WHEN DATE_PART(day, m.date_utc) = 31 THEN 0.0
       WHEN DATE_PART(month, m.date_utc) = 2 AND m.date_utc = LAST_DAY(m.date_utc)
           THEN p.mrc / 30.0 * (31 - DATE_PART(day, m.date_utc))
       ELSE p.mrc / 30.0
    END::FLOAT AS mrc_cost,
    p.data_size::FLOAT / 30.0 AS mb_contribution_to_pool,
    p.mou_size::FLOAT / 30.0 AS mou_contribution_to_pool
FROM {{ ref('carrier_subscriptions_mdn_daily_rateplan') }} m
JOIN {{ ref('subscription_rate_plans') }} p ON (m.subscription_rate_plan_code = p.code)
WHERE
    (date_utc >= {{ var('ds') }}::DATE - INTERVAL '7 DAYS')
    ---when both are null execute the query as such else compare the first non null value
    AND (m.date_utc >= COALESCE(p.plan_start_date, '0001-01-01'))
    AND (m.date_utc <= COALESCE(p.plan_end_date, '9999-12-31'))

UNION ALL SELECT
    i.mdn,
    i.date_utc,
    CASE
        WHEN r.network = 0 THEN 'Sprint'
        WHEN r.network = 1 THEN 'Kore'
        WHEN r.network = 2 THEN 'ATT'
        ELSE 'OTHER'
    END AS network,
    CASE
        WHEN r.network = 0 THEN 'Sprint'
        WHEN r.network = 1 THEN 'Kore'
        WHEN r.network = 2 THEN 'ATT'
        ELSE 'OTHER'
    END AS carrier,
    CASE
        -- For Sprint, all plans with the same MRC cost form a single pool
        WHEN r.network = 0 THEN 'Pool' || to_char(r.mrc, '90D99')
        -- For Kore, the combination of plan and LTE/non-LTE form a single pool
        WHEN r.network = 1 THEN code || CASE WHEN lte = 1 THEN '-LTE' ELSE '' END
    END AS pool_name,
    i.item_uuid,
    NULL AS iccid,
    i.imsi,
    r.subscription_rate_plan_id,
    r.code,
    /* MRC cost actually doesn’t depend on number of days in month.
       This implies that 31st day of a month is free, and we are paying for Feb 29/30 even
       if they don’t exist
     */
    CASE
        WHEN date_part(day, i.date_utc) = 31 THEN 0.0
        -- on the last day of Feb, we add on the daily cost for Feb 29/30 as well
        WHEN date_part(month, i.date_utc) = 2 AND i.date_utc = last_day(i.date_utc)
            THEN r.mrc / 30.0 * (31 - date_part(day, i.date_utc))
        ELSE r.mrc / 30.0
    END::FLOAT AS mrc_cost,
    -- overage charges calculated using 30-day month
    r.data_size::float / 30.0 AS mb_contribution_to_pool,
    r.mou_size::float / 30.0 AS mou_contribution_to_pool
FROM {{ ref('inventory_mdn_daily_rateplan') }} i
JOIN {{ ref('subscription_rate_plans') }} r ON i.subscription_rate_plan_id = r.subscription_rate_plan_id
WHERE
    (i.date_utc >= {{ var('ds') }}::DATE - INTERVAL '7 DAYS')
    ---when both are null execute the query as such else compare the first non null value
    AND (i.date_utc >= COALESCE(r.plan_start_date, '0001-01-01'))
    AND (i.date_utc <= COALESCE(r.plan_end_date, '9999-12-31'))
ORDER BY date_utc
