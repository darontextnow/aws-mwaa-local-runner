{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

SELECT
    date_utc,
    client_type,
    SUM(revenue) AS ad_revenue
FROM {{ ref('advertising_line_item_ad_unit_daily_revenue') }}
WHERE (date_utc < CURRENT_DATE)

{% if is_incremental() %}
    AND (date_utc >= (SELECT MAX(date_utc) - interval '1 week' FROM {{ this }}))
{% endif %}

{% if target.name == 'dev' %}
    AND (date_utc >= CURRENT_DATE - INTERVAL '1 week')
{% endif %}

GROUP BY 1, 2
