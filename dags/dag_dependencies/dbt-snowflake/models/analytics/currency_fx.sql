{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

WITH usdcad AS (
    -- `merchant_currency` is set to CAD in Google; if we wanted to convert currencies
    -- into CAD in our output, we could have just used `currency_cvr` directly.
    -- However since we want to convert every currency into USD, we need to compute
    -- USD/CAD as an extra conversion factor
    SELECT
        order_ts::DATE AS dt,
        AVG(currency_cvr) AS usd_to_cad
    FROM {{ source('google_play', 'google_play_earnings') }}
    WHERE (currency = 'USD') AND (merchant_currency = 'CAD')
    GROUP BY dt
)
SELECT
    date_utc,
    currency,
    COALESCE(fx, LAG(fx) IGNORE NULLS OVER (PARTITION BY currency ORDER BY date_utc)) AS fx_usd
FROM {{ source('support', 'dates') }}
FULL JOIN (
    SELECT DISTINCT currency FROM {{ source('google_play', 'google_play_earnings') }}
) all_currencies ON (TRUE)
LEFT JOIN (
    SELECT order_ts::DATE AS date_utc,
           currency,
           AVG(currency_cvr / usd_to_cad) AS fx
    FROM {{ source('google_play', 'google_play_earnings') }}
    JOIN usdcad ON (order_ts::DATE = dt)
    WHERE (merchant_currency = 'CAD')
    GROUP BY 1, 2
) curr USING (date_utc, currency)
WHERE (date_utc BETWEEN '2012-12-21' AND CURRENT_DATE)
