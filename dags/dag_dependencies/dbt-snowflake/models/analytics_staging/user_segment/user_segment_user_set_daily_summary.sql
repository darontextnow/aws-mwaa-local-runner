{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}

WITH user_set_tn_type AS (
    SELECT 
        date_utc,
        user_set_id,
        sub_type,
        consumable_type,
        ad_upgrade_type,
        phone_num_upgrade_type
    FROM {{ ref('user_segment_user_set_daily_tn_type') }}
    WHERE

    {% if is_incremental() or target.name == 'dev' %}
        (date_utc >= {{ var('ds') }}::DATE - INTERVAL '7 DAYS')
    {% else %}
        (date_utc BETWEEN '2020-01-01' AND {{ var('ds') }}::DATE)
    {% endif %}

),

user_set_rfm AS (
    SELECT 
        date_utc,
        user_set_id,
        rfm_segment
    FROM {{ ref('user_segment_user_set_daily_rfm') }} 
    WHERE

    {% if is_incremental() or target.name == 'dev' %}
        (date_utc >= {{ var('ds') }}::DATE - INTERVAL '7 DAYS')
    {% else %}
        (date_utc BETWEEN '2020-01-01' AND {{ var('ds') }}::DATE)
    {% endif %}

)

SELECT 
    date_utc,
    user_set_id,
    COALESCE(sub_type, 'Non-Sub') AS sub_type,
    consumable_type,
    ad_upgrade_type,
    phone_num_upgrade_type,
    CASE WHEN rfm_segment IS NULL THEN 'other_users' ELSE rfm_segment END AS rfm_segment
FROM user_set_tn_type as tn_type
FULL OUTER JOIN user_set_rfm as rfm USING (date_utc, user_set_id)
