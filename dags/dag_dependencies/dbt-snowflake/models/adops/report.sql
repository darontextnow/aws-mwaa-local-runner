{{
    config(
        tags=['daily'],
        materialized='incremental',
        snowflake_warehouse='PROD_WH_MEDIUM',
        unique_key='date'
    )
}}

SELECT
    date,
    app,
    partner AS network,
    platform,
    ad_type,
    ad_format,
    ad_tech_platform AS custom_3,
    revenue_channel AS custom_4,
    CASE
        ---AdopsRevenue Team confirmed the country empty or null is mapped to USA
        WHEN country = 'United States of America' OR TRIM(country) = '' OR country IS NULL THEN 'US'
        WHEN country = 'Canada' THEN 'CA'
        WHEN country = 'Mexico' THEN 'MX'
        ELSE country
    END AS country,
    SUM(impressions) AS impressions,
    SUM(net_revenue_usd) AS revenue,
    NULL clicks,
    NULL custom_1,
    NULL custom_2,
    NULL requests,
    NULL iap_revenue
FROM {{ source('core', 'burt_report') }}
WHERE
    --Set start date to 2024-12-01 to ensure historical data loaded by libring before this date remains unaffected
    (date >= '2024-12-01'::DATE)
{% if is_incremental() %}
    AND (date >= {{ var('ds') }}::DATE - INTERVAL '30 DAYS')
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
