/*
 Note that we are only using line-items with 'ACTIVATED' as the activation status, this state column
 is not available in other data sources (
 */

{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH hist AS (

    SELECT mdn,
           imsi,
           iccid,
           carrier,
           subscription_rate_plan_code,
           from_ts,
           to_ts
    FROM {{ ref('carrier_subscriptions_iccid_rateplan_history') }}
    WHERE activation_status = 'ACTIVATED'

),

daily AS (

    SELECT date_utc,
           hist.mdn,
           hist.imsi,
           hist.iccid,
           hist.carrier,
           hist.subscription_rate_plan_code,
           ROW_NUMBER() OVER (
               PARTITION BY mdn, date_utc
               ORDER BY from_ts DESC
           ) AS rank
    FROM hist
    {{
        history_to_daily_join('hist.from_ts', 'hist.to_ts')
    }}
    WHERE date_utc < CURRENT_DATE

)

SELECT a.mdn,
       a.date_utc,
       a.imsi,
       a.iccid,
       b.inventory_uuid,
       a.carrier,
       a.subscription_rate_plan_code
FROM daily a
LEFT JOIN {{ source('tmobile', 'carrier_subscription_devices') }} b
  ON a.iccid = b.iccid
WHERE rank = 1
