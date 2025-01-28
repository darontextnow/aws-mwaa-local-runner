{{
    config(
        tags=['daily'],
        materialized='view',
        unique_key='date_utc'
    )
}}


SELECT 
    date AS date_utc,
    CASE
        WHEN UPPER(platform) IN ('TN_ANDROID', '2L_ANDROID') THEN 'ANDROID (INCL 2L)'
        ELSE platform
    END AS CLIENT_TYPE,
    SUM(impressions) AS TOTAL_IMPRESSIONS
FROM {{ ref('report') }}
WHERE 
    (UPPER(platform) IN ('TN_IOS_FREE', 'TN_ANDROID', '2L_ANDROID'))
    AND (date_utc >= '2021-01-01'::DATE)
GROUP BY 1, 2
ORDER BY 1 DESC