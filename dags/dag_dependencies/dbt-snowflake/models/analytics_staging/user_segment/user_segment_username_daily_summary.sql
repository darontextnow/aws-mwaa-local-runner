{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

WITH username_tn_type AS (
    SELECT 
        date_utc,
        username,
        sub_type,
        consumable_type,
        ad_upgrade_type,
        phone_num_upgrade_type
    FROM {{ ref('user_segment_username_daily_tn_type') }}
    WHERE

    {% if is_incremental() or target.name == 'dev' %}
        (date_utc = {{ var('ds') }}::DATE)
    {% else %}
        (date_utc BETWEEN '2020-01-01' AND {{ var('ds') }}::DATE)
    {% endif %}
),
username_rfm AS (
    SELECT 
        date_utc,
        username,
        rfm_segment
    FROM {{ ref('user_segment_username_daily_rfm') }}
    WHERE

    {% if is_incremental() or target.name == 'dev' %}
        (date_utc = {{ var('ds') }}::DATE)
    {% else %}
        (date_utc BETWEEN '2020-01-01' AND {{ var('ds') }}::DATE)
    {% endif %}
)

SELECT 
    date_utc,
    username,
    COALESCE(sub_type, 'Non-Sub') AS sub_type,
    consumable_type,
    ad_upgrade_type,
    phone_num_upgrade_type,
    CASE WHEN rfm_segment IS NULL THEN 'other_users' ELSE rfm_segment END AS rfm_segment
FROM username_tn_type AS tn_type
FULL OUTER JOIN username_rfm AS rfm USING (date_utc, username)
