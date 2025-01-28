{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_LARGE',
        post_hook = [ "DELETE FROM {{ this }}
            USING (SELECT set_uuid AS bad_set_uuid FROM {{ ref('bad_sets') }})
            WHERE (user_set_id = bad_set_uuid)"
        ]
    )
}}

-- Web users are included and using username as user_set_id
-- Bad sets and disabled web users are excluded
-- Mobile users without an invalid user set id (ie. dau = 0 ) will be excluded

SELECT
    da.date_utc,
    da.user_set_id,
    client_type,
    COALESCE(sub_type, 'Non-Sub') AS sub_type,
    consumable_type,
    ad_upgrade_type,
    phone_num_upgrade_type,
    COALESCE(rfm_segment, 'other_users') AS rfm_segment,
    dau
FROM (
    SELECT date_utc, user_set_id, client_type, SUM(dau) AS dau
    FROM {{ ref('user_set_daily_activities') }}
    WHERE

    {% if is_incremental() %}
        (date_utc BETWEEN {{ var('ds') }}::DATE - INTERVAL '90 DAYS' AND {{ var('ds') }}::DATE)
    {% else %}
        (date_utc BETWEEN '2020-01-01'::DATE AND {{ var('ds') }}::DATE)
    {% endif %}

        AND (user_set_id NOT IN (SELECT set_uuid FROM {{ ref('bad_sets') }}))
        AND (dau > 0)
    GROUP BY 1, 2, 3
) da
LEFT JOIN {{ ref('user_segment_user_set_daily_summary') }} ds ON
    (da.date_utc = ds.date_utc)
    AND (da.user_set_id = ds.user_set_id)

{% if target.name == 'dev' %}
    AND (da.date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 WEEK')
{% endif %}

ORDER BY 1
