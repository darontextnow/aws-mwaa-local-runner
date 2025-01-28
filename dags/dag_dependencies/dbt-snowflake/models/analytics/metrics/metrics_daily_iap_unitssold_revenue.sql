{{
    config(
        tags=['daily'],
        materialized='view',
        unique_key='date_utc'
    )
}}


SELECT 
    date_utc,
    CASE WHEN UPPER(client_type) IN ('TN_ANDROID','2L_ANDROID') THEN 'ANDROID (INCL 2L)'
    ELSE CLIENT_TYPE END AS CLIENT_TYPE,
    product_category,
    SUM(net_units_sold) AS daily_units_sold,
    SUM(net_revenue) AS daily_iap_revenue
FROM {{ ref('metrics_daily_iap_revenue') }}
WHERE 
    (UPPER(country_code) IN ('US', 'CA'))
    AND (UPPER(client_type) IN ('TN_IOS_FREE', 'TN_ANDROID', '2L_ANDROID'))
    AND (date_utc >= '2021-01-01')
GROUP BY 1, 2, 3
ORDER BY 1 DESC