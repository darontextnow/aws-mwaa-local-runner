{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}

SELECT

    coalesce(a."client_details.client_data.user_data.username", u.username)   AS username,
    a.user_id_hex,
    a.created_at                                                              AS updated_at,
    a.created_at::date                                                        AS date_utc,
    a."payload.age_range"                                                     AS age_range,
    a."payload.gender"                                                        AS gender

 FROM {{ source('party_planner_realtime', 'user_updates') }} a
 LEFT JOIN {{ ref('analytics_users') }} u USING (user_id_hex)

-- ignore unknown values and NULL strings
WHERE (age_range != 'AGE_RANGE_UNKNOWN' OR gender != 'GENDER_UNKNOWN')
AND COALESCE(len(age_range), 0) + COALESCE(len(gender), 0) != 0
        
{% if is_incremental() %}

  AND a.created_at  >= (SELECT MAX(updated_at) - INTERVAL '7 days' FROM {{ this }})

{% endif %}

{% if target.name == 'dev' %}

  AND a.created_at  >= CURRENT_DATE - INTERVAL '1 week'

{% endif %}