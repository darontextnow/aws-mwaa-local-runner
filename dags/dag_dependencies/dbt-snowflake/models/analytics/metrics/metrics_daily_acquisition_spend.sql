{{
    config(
        tags=['daily'],
        materialized='view',
        unique_key='date_utc'
    )
}}


WITH adjust_ua_cte AS (
    SELECT 
        CAST(cohort_utc AS DATE) AS date_utc,
        CASE
            WHEN UPPER(CLIENT_TYPE) IN ('TN_ANDROID', '2L_ANDROID') THEN 'ANDROID (INCL 2L)'
            ELSE CLIENT_TYPE
        END AS CLIENT_TYPE,
        SUM(cost) AS ua_cost
    FROM {{ref('ua_cost_attribution_adjustment')}}
    WHERE 
        UPPER(CLIENT_TYPE) IN ('TN_ANDROID', 'TN_IOS_FREE', '2L_ANDROID')
        AND UPPER(country_code) IN ('US', 'CA')
        AND CAST(cohort_utc AS DATE) >= '2021-01-01'
        AND  CAST(cohort_utc AS DATE) < '2023-09-01'
    GROUP BY 1, 2

    UNION ALL

    SELECT 
        c.date AS date_utc,
        CASE WHEN c.site_public_id = 'com.enflick.android.TextNow' THEN 'ANDROID (INCL 2L)'
             WHEN c.site_public_id = '314716233' THEN 'TN_IOS_FREE'
        END AS client_type,
        SUM(adn_cost) AS ua_cost
    FROM  {{ source('singular', 'marketing_data') }} AS c
    WHERE c.date < CURRENT_DATE
          AND UPPER(c.country_field) IN ('USA', 'CAN')
          AND  CAST(c.date AS DATE) >= '2023-09-01'
    GROUP BY 1, 2


),
tatari_ua_cte AS (
    SELECT 
        CAST(spot_datetime AS DATE) AS date_utc,
        'TATARI' AS client_type,
        SUM(spend) AS ua_cost
    FROM {{ source('ua', 'tatari_linear_spend_and_impressions') }}
    WHERE date_utc >= '2021-01-01'
    GROUP BY 1
)
SELECT 
    date_utc,
    client_type,
    ua_cost
FROM adjust_ua_cte
UNION ALL
SELECT 
    date_utc,
    client_type,
    ua_cost
FROM tatari_ua_cte
ORDER BY 1 DESC