{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}


WITH ild_purchasers AS (
    -- international long distance calling credits purchasers
    -- Assume ILD credits are active for 30 days
    (
        SELECT DISTINCT
            dates.date_utc AS date_utc,
            username,
            'ILD Credits (Internal)' AS consumable_type
        FROM {{ source('support', 'dates') }}
        CROSS JOIN (
            SELECT DISTINCT
                created_at::DATE AS date_utc,
                username
            FROM {{ ref ('billing_service_purchases') }}
            WHERE
                (date_utc < CURRENT_DATE)
                AND (item_category = 'credit')
                AND (paid_amount > 0)

            {% if is_incremental() or target.name == 'dev' %}
                AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 WEEK')
            {% endif %}

        ) AS ild

        WHERE
            (dates.date_utc BETWEEN ild.date_utc AND DATEADD(DAY, 30, ild.date_utc)) -- assume ILD credits are active for 30 days
            AND (dates.date_utc < CURRENT_DATE)
    )

    UNION SELECT DISTINCT
        date_utc,
        username,
        'ILD Credits (IAP)' AS consumable_type
    FROM {{ ref('iap_user_daily_subscription') }}
    WHERE
        (feature ILIKE 'International Credit%')
        AND (date_utc < CURRENT_DATE)

    {% if is_incremental() or target.name == 'dev' %}
        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 WEEK')
    {% endif %}

),

upgrade_purchasers AS (
    SELECT date_utc,
        username,
        MIN(CASE WHEN feature ILIKE 'Ad FREE+%' OR feature ILIKE 'Ad Free Lite%' THEN tn_type_priority ELSE NULL END)           AS highest_ad_upgrade_priority,
        MIN(CASE WHEN feature ILIKE 'Premium Number%' OR feature ILIKE 'Lock In Number%' THEN tn_type_priority ELSE NULL END)   AS highest_phone_num_upgrade_priority
    FROM {{ ref('iap_user_daily_subscription') }}
    JOIN {{ ref('user_segment_tn_type_priority') }} ON (feature = tn_type)
    WHERE (date_utc < CURRENT_DATE)

    {% if is_incremental() or target.name == 'dev' %}
        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 WEEK')
    {% endif %}

    GROUP BY 1, 2
),

subscribers AS (
        -- NWTT and subscribers
    SELECT DISTINCT
        date_utc,
        username,
        CASE WHEN plan_family IN ('NWTT', 'Talk & Text') THEN 'no data' ELSE 'with data' END AS data_segment,
        CASE WHEN plan_name like 'Employee Unlimited%' THEN 'TN Employee' WHEN is_free THEN 'free' ELSE 'paid' END AS paying_segment
    FROM {{ ref('base_user_daily_subscription') }}
    WHERE (date_utc < CURRENT_DATE)

    {% if is_incremental() or target.name == 'dev' %}
        AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 WEEK')
    {% endif %}

)

SELECT DISTINCT
    date_utc,
    username,
    COALESCE(CASE WHEN ad_upgrade_p.tn_type is not null THEN 'paid' ELSE paying_segment END || '/' || data_segment, 'Non-Sub') AS sub_type,
    ild_purchasers.consumable_type  AS consumable_type,
    ad_upgrade_p.tn_type            AS ad_upgrade_type,
    phone_num_p.tn_type             AS phone_num_upgrade_type
FROM subscribers
FULL OUTER JOIN ild_purchasers USING (date_utc, username)
FULL OUTER JOIN upgrade_purchasers USING (date_utc, username)
LEFT JOIN {{ ref('user_segment_tn_type_priority') }} AS ad_upgrade_p ON highest_ad_upgrade_priority = ad_upgrade_p.tn_type_priority
LEFT JOIN {{ ref('user_segment_tn_type_priority') }} AS phone_num_p ON highest_phone_num_upgrade_priority = phone_num_p.tn_type_priority
WHERE
    (username NOT IN (
        SELECT username FROM {{ source('core', 'users') }} WHERE (account_status IN ('DISABLED', 'HARD_DISABLED'))))

{% if is_incremental() or target.name == 'dev' %}
    AND (date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 WEEK')
{% endif %}

ORDER BY 1
