{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}

-- The relationship between username and use cases is 1:M
-- Cannot join users to grab username because the table is nested

SELECT

    a."client_details.client_data.user_data.username" as username,
    a.user_id_hex,
    a.created_at AS updated_at,
    a.created_at::date AS date_utc,
    CASE WHEN parse_json(F.value):element = 9 
        THEN 'TN_USE_CASE_OTHER' ELSE parse_json(F.value):element::varchar END AS use_case

FROM {{ source('party_planner_realtime', 'user_updates') }} a,
    TABLE(FLATTEN(parse_json(a."payload.tn_use_cases"):"list")) F
WHERE use_case IS NOT NULL AND use_case != ''

{% if is_incremental() %}

  AND a.created_at>= (SELECT MAX(updated_at) - interval '7 days' FROM {{ this }})

{% endif %}

{% if target.name == 'dev' %}

  AND a.created_at >= CURRENT_DATE - INTERVAL '1 week'

{% endif %}