{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

SELECT
    username,
    created_at::DATE AS date_utc,
    client_type,
    SUM(NVL(term_cost, 0)) AS termination_cost,    -- caller pays
    SUM(NVL(layered_paas, 0)) AS layered_paas_cost -- both caller and recipient pay
FROM {{ source('loadr', 'oncallend_callbacks') }}
WHERE
    (username <> '')
    AND (client_type <> '')
    AND (created_at < CURRENT_DATE)

{% if is_incremental() %}
    AND (created_at >= (SELECT MAX(date_utc) - interval '7 days' FROM {{ this }}))
{% endif %}

{% if target.name == 'dev' %}
    AND (created_at::date >= CURRENT_DATE - INTERVAL '1 week')
{% endif %}

GROUP BY 1, 2, 3
ORDER BY 2
