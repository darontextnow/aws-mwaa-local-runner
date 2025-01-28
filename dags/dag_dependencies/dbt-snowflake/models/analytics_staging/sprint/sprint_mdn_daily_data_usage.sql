/* For data before 2018-08-20 we use data from Data Usage Service (core.sprint_data_usage)
   For data on or after 2018-08-20 we use the sprint_ams tables (core.sprint_cdma_data_usage and
   core.sprint_lte_data_usage)
 */

{{
    config(
        tags=['daily'],
        materialized='table'
    )
}}

WITH legacy_usage AS (
    SELECT
        mdn,
        date(usage_time) as date_utc,
        sum(case when type = 'LteDataUsage' then incoming_bytes else 0 end) as lte_incoming_bytes,
        sum(case when type = 'LteDataUsage' then outgoing_bytes else 0 end) as lte_outgoing_bytes,
        sum(case when type = 'IpdrDataUsage' then incoming_bytes else 0 end) as ipdr_incoming_bytes,
        sum(case when type = 'IpdrDataUsage' then outgoing_bytes else 0 end) as ipdr_outgoing_bytes
    FROM {{ source('sprint_ams', 'sprint_data_usage') }}
    WHERE usage_time < '2018-08-20'
    group by 1,2
),

cdma_usage AS (

    SELECT
        mdn,
        reported_at as date_utc,
        0 AS lte_incoming_bytes,
        0 AS lte_outgoing_bytes,
        sum(rx_bytes) as ipdr_incoming_bytes,
        sum(tx_bytes) as ipdr_outgoing_bytes
    FROM {{ source('sprint_ams', 'sprint_cdma_data_usage') }}
    WHERE reported_at >= '2018-08-20'
    group by 1,2

),

lte_usage AS (

    /* In the `lte_usage_records` table there is a column named `network_type`, and
       if the column value is set to 1 this means that we received a usage record in
       the LTE data feed for EHRPD data usage which is actually CDMA data usage.
       If the column value is set to 0 this means that we received a usage record in the
       LTE data feed for LTE data usage
     */

    WITH hist_to_daily as (
        SELECT
            mdn,
            reported_at as date_utc,
            network_type,
            rx_bytes,
            tx_bytes,
            row_number() over (partition by mdn, reported_at, apn, network_type order by from_ts desc) as rank
        FROM {{ source('sprint_ams', 'sprint_lte_data_usage') }}
        LEFT JOIN {{ ref('inventory_item_rateplan_history') }} as h ON
            sprint_lte_data_usage.imsi = h.imsi AND
            h.from_ts < dateadd(day, 1, sprint_lte_data_usage.reported_at) AND
            coalesce(h.to_ts, CURRENT_TIMESTAMP) > sprint_lte_data_usage.reported_at
        WHERE reported_at >= '2018-08-20'
    )
    SELECT
        mdn,
        date_utc,
        sum(case when network_type = 0 then rx_bytes else 0 end) as lte_incoming_bytes,
        sum(case when network_type = 0 then tx_bytes else 0 end) as lte_outgoing_bytes,
        sum(case when network_type = 1 then rx_bytes else 0 end) as ipdr_incoming_bytes,
        sum(case when network_type = 1 then tx_bytes else 0 end) as ipdr_outgoing_bytes
    from hist_to_daily
    WHERE rank = 1
    group by 1,2

),

all_data AS (

    SELECT * FROM legacy_usage

    UNION ALL

    SELECT * FROM cdma_usage

    UNION ALL

    SELECT * FROM lte_usage

)

SELECT
    mdn,
    date_utc,
    sum(lte_incoming_bytes) as lte_incoming_bytes,
    sum(lte_outgoing_bytes) as lte_outgoing_bytes,
    sum(ipdr_incoming_bytes) as ipdr_incoming_bytes,
    sum(ipdr_outgoing_bytes) as ipdr_outgoing_bytes,
    sum(lte_incoming_bytes) + sum(lte_outgoing_bytes) + sum(ipdr_incoming_bytes) + sum(ipdr_outgoing_bytes) as total_bytes
FROM all_data
GROUP by mdn, date_utc
ORDER BY date_utc
