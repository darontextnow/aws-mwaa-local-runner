{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE'

    )
}}

WITH dau_userset_cte AS (
    SELECT
        date_utc,
        client_type,
        first_country_code AS country_code,
        user_set_id,
        dau
    FROM {{ ref('growth_dau_by_segment') }}
    LEFT JOIN {{ ref('user_sets') }} USING(user_set_id)
    WHERE
        client_type IN ('TN_ANDROID', 'TN_IOS_FREE', '2L_ANDROID','TN_WEB')
        AND first_country_code IN ('US', 'CA')
        AND DATE_UTC < CURRENT_DATE

    {% if is_incremental() %}
        AND date_utc >= (SELECT MAX(date_utc) FROM {{ this }}) - INTERVAL '30 days'
    {% endif %}
),
dau_level_cte AS (
    SELECT
        date_utc,
        client_type,
        country_code,
        SUM(dau) AS total_dau
    FROM dau_userset_cte
    GROUP BY 1, 2, 3
),
dau_generated_revenue_cte AS (
    SELECT
        date_utc,
        client_type,
        country_code,
        SUM(adjusted_revenue) AS daily_ad_revenue,
        SUM(dau) AS total_dau_generated_revenue
    FROM dau_userset_cte
    JOIN {{ ref('analytics_users') }} USING (user_set_Id)
    JOIN {{ ref('revenue_user_daily_ad') }} USING (date_utc, username, client_type)
    GROUP BY 1,2,3
    HAVING SUM(adjusted_revenue) > 0
)
SELECT
    date_utc,
    CASE WHEN UPPER(client_type) IN ('TN_ANDROID','2L_ANDROID') THEN 'ANDROID (INCL 2L)'
    ELSE client_type END AS client_type,
    country_code,
    SUM(total_dau) AS total_dau,
    SUM(total_dau_generated_revenue) AS total_dau_generated_revenue,
    CURRENT_TIMESTAMP AS inserted_timestamp
FROM dau_generated_revenue_cte
FULL OUTER JOIN dau_level_cte USING (date_utc, client_type, country_code)
GROUP BY 1, 2, 3