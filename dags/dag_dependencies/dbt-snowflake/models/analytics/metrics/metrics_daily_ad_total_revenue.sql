{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'

    )
}}

WITH AD_REVENUE_CTE AS (
    SELECT date_utc,
        CASE WHEN UPPER(client_type) IN ('TN_ANDROID','2L_ANDROID') THEN 'ANDROID (INCL 2L)'
        ELSE client_type END AS client_type,
        FIRST_country_code AS country_code,
        SUM(adjusted_revenue) AS daily_ad_revenue
    FROM {{ ref('revenue_user_daily_ad') }}
    LEFT JOIN {{ ref('analytics_users') }} USING (username)
    LEFT JOIN {{ ref('user_sets') }} USING (user_set_Id)
    
    {% if is_incremental() %}
        WHERE (date_utc >= (SELECT MAX(date_utc) FROM {{ this }}) - INTERVAL '30 days')
    {% endif %}
    
    GROUP BY 1, 2, 3
),
iap_revenue_cte AS (
    SELECT 
        date_utc,
        CASE WHEN UPPER(client_type) IN ('TN_ANDROID','2L_ANDROID') THEN 'ANDROID (INCL 2L)'
            ELSE client_type END AS client_type,
        country_code,
        SUM(net_revenue) AS daily_iap_revenue
    FROM {{ ref('metrics_daily_iap_revenue') }}
    WHERE 
        (country_code IN ('US', 'CA'))
        AND (client_type IN ('TN_IOS_FREE', 'TN_ANDROID', '2L_ANDROID'))
        
        {% if is_incremental() %}
            AND (date_utc >= (SELECT MAX(date_utc) FROM {{ this }}) - INTERVAL '30 days')
        {% endif %}
        
    GROUP BY 1, 2, 3
)
SELECT 
    date_utc,
    client_type,
    country_code,
    NVL(daily_ad_revenue,0) AS daily_ad_revenue,
    NVL(daily_iap_revenue,0) AS daily_iap_revenue,
    NVL(daily_ad_revenue,0) + NVL(daily_iap_revenue,0) AS total_revenue,
    CURRENT_TIMESTAMP AS inserted_timestamp
FROM AD_REVENUE_CTE
FULL OUTER JOIN iap_revenue_cte USING (date_utc, client_type, country_code)
