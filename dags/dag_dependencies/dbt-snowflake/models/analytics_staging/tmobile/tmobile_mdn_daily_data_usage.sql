/*
 Note that we include all TMO data usage here. PWG happens to be the only partner we have for TMO carrier
 subscriptions so we are only including data usage from them. If we add more partnerships down the road,
 we need to add (union) additional partner data usage in the subquery.
 */

{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

SELECT
    mdn,
    date_utc,
    carrier,
    SUM(rx_bytes) AS total_received_bytes,
    SUM(tx_bytes) AS total_transmitted_bytes,
    SUM(rx_bytes) + SUM(tx_bytes) AS total_bytes
FROM (
    SELECT
        mdn,
        reported_at AS date_utc,
        'PWG' AS carrier,
        rx_bytes,
        tx_bytes,
        ROW_NUMBER() OVER (PARTITION BY mdn, reported_at order by reported_at) AS rank
    FROM {{ source('tmobile', 'tmobile_pwg_data_usage') }}
    WHERE ((rx_bytes > 0) OR (tx_bytes > 0))
) g
WHERE (rank = 1)
GROUP BY 1, 2, 3
ORDER BY 2
