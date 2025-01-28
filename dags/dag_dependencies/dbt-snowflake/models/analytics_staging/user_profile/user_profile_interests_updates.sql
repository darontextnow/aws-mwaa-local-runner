{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc'
    )
}}

-- The relationship between username and user interests is 1:M
-- Cannot join users to grab username because the table is nested

SELECT

    a."client_details.client_data.user_data.username" AS username,
    a.user_id_hex,
    a.created_at::date AS date_utc,
    a.created_at as updated_at,
    parse_json(F.value):element::varchar AS interest

FROM {{ source('party_planner_realtime', 'user_updates') }} a,
    TABLE(FLATTEN(parse_json(a."payload.interests"):"list")) F
WHERE interest IS NOT NULL AND interest != '' 

{% if is_incremental() %}

  AND a.created_at>= (SELECT MAX(updated_at) - INTERVAL '7 days' FROM {{ this }})

{% endif %}

{% if target.name == 'dev' %}

  AND a.created_at >= CURRENT_DATE - INTERVAL '1 week'

{% endif %}