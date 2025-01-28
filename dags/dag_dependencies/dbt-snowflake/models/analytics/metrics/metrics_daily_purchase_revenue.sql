{{
    config(
        tags=['daily'],
        materialized='table',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

SELECT
    date_utc,
    'OTHER' AS client_type,
    {{ dbt_utils.pivot(
        'item_category',
        dbt_utils.get_column_values(ref('revenue_user_daily_purchase'), 'item_category', default='UNKNOWN'),
        suffix='_revenue',
        then_value='revenue'
    ) }}
FROM {{ ref('revenue_user_daily_purchase') }}
WHERE (date_utc < CURRENT_DATE)

{% if target.name == 'dev' %}
    AND (date_utc >= CURRENT_DATE - INTERVAL '1 week')
{% endif %}

GROUP BY 1
