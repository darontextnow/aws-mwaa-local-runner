{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}

-- (Wireless Subs > NWTT Subs > Free; Ad Free+ > Ad Free Lite; Premium Number > Lock In Number; Annual > Monthly)

WITH highest_tn_type_by_user_set AS (
    SELECT 
        date_utc,
        set_uuid AS user_set_id,
        MIN(sub.tn_type_priority) AS sub_highest_priority,
        MIN(consumable.tn_type_priority) AS consumable_highest_priority,
        MIN(ad_upgrade.tn_type_priority) AS ad_upgrade_highest_priority,
        MIN(phone_num_upgrade.tn_type_priority) AS phone_num_upgrade_highest_priority
    FROM {{ ref('user_segment_username_daily_tn_type') }} 
    JOIN {{ source('dau', 'user_set') }} USING (username)
    LEFT JOIN {{ ref('user_segment_tn_type_priority') }} sub ON (sub_type = sub.tn_type)
    LEFT JOIN {{ ref('user_segment_tn_type_priority') }} consumable ON (consumable_type = consumable.tn_type)
    LEFT JOIN {{ ref('user_segment_tn_type_priority') }} ad_upgrade ON (ad_upgrade_type = ad_upgrade.tn_type)
    LEFT JOIN {{ ref('user_segment_tn_type_priority') }} phone_num_upgrade ON (phone_num_upgrade_type = phone_num_upgrade.tn_type)
    WHERE (date_utc < {{ var('current_date') }})

    {% if is_incremental() or target.name == 'dev' %}
        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '7 DAYS')
    {% endif %}

    GROUP BY 1, 2
)

SELECT
    date_utc,
    user_set_id,
    sub_p.tn_type AS sub_type,
    consumable_p.tn_type AS consumable_type,
    ad_upgrade_p.tn_type AS ad_upgrade_type,
    phone_num_p.tn_type AS phone_num_upgrade_type
FROM highest_tn_type_by_user_set
LEFT JOIN {{ ref('user_segment_tn_type_priority') }} AS sub_p ON sub_highest_priority = sub_p.tn_type_priority
LEFT JOIN {{ ref('user_segment_tn_type_priority') }} AS consumable_p ON consumable_highest_priority = consumable_p.tn_type_priority
LEFT JOIN {{ ref('user_segment_tn_type_priority') }} AS ad_upgrade_p ON ad_upgrade_highest_priority = ad_upgrade_p.tn_type_priority
LEFT JOIN {{ ref('user_segment_tn_type_priority') }} AS phone_num_p ON phone_num_upgrade_highest_priority = phone_num_p.tn_type_priority
ORDER BY 1
