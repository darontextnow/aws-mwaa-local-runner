/*
    This table keeps a history of mdn / rate plan assigned to every iccid (SIM card)
    Note that we are only ingesting PWG records for now, which is our vendor for TMO
    carrier subscription plans. In case more vendors are added in the future, we need
    to make corresponding changes here.
*/

{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='surrogate_key'
    )
}}

WITH carrier_sub_history AS (

    SELECT iccid AS iccid,
           mdn AS mdn,
           imsi AS imsi,
           carrier,
           concat (carrier , carrier_plan_id ) AS subscription_rate_plan_code,
           activation_status AS activation_status,
           deleted_at AS deleted_at,
           COALESCE(updated_at, created_at) AS event_ts
    FROM {{ source('tmobile', 'carrier_subscriptions_history') }}
    WHERE carrier IN ('PWG')
),

{{
    log_to_history_cte(
        log_table='carrier_sub_history',
        id_column='iccid',
        state_columns=['mdn', 'imsi', 'carrier', 'subscription_rate_plan_code', 'activation_status'],
        ts_column='event_ts',
        aux_columns=['deleted_at'],
        order_by=['event_ts']
    )
}}

SELECT {{ dbt_utils.generate_surrogate_key(['iccid', 'from_ts']) }} AS surrogate_key,
       iccid,
       mdn,
       imsi,
       carrier,
       subscription_rate_plan_code,
       activation_status,
       from_ts,
       least(coalesce(to_ts, deleted_at), coalesce(deleted_at, to_ts)) AS to_ts
FROM log_to_history
WHERE mdn <> ''
  AND subscription_rate_plan_code <> ''
