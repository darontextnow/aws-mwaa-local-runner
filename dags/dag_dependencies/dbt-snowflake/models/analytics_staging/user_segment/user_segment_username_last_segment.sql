{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='username'
    )
}}

WITH last_date_for_user AS (
    SELECT username, MAX(date_utc) AS date_utc
    FROM {{ ref('user_segment_username_daily_summary') }}
    WHERE
        (date_utc <= CURRENT_DATE)

    {% if is_incremental() %}
        AND (date_utc >= CURRENT_DATE - INTERVAL '7 DAYS')
    {% endif %}

        AND (username IS NOT NULL) AND (username <> '')
        AND (COALESCE(NULLIF(rfm_segment, ''),
                     NULLIF(sub_type, ''),
                     NULLIF(consumable_type, ''),
                     NULLIF(ad_upgrade_type, ''),
                     NULLIF(phone_num_upgrade_type, '')) IS NOT NULL)
    GROUP BY 1
)
SELECT username,
       rfm_segment,
       sub_type,
       consumable_type,
       ad_upgrade_type,
       phone_num_upgrade_type,
       date_utc AS last_updated
FROM {{ ref('user_segment_username_daily_summary') }}
JOIN last_date_for_user USING (username, date_utc)
