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
    a."payload.location.location_source"                                      AS location_source,
    a."payload.location.continent_code"                                       AS continent_code,
    a."payload.location.country_code"                                         AS country_code,
    a."payload.location.state_code"                                           AS state_code,
    a."payload.location.city"                                                 AS city,
    a."payload.location.zip_code"                                             AS zip_code,
    a."payload.location.area_code"                                            AS area_code

 FROM {{ source('party_planner_realtime', 'user_updates') }} a
 LEFT JOIN {{ ref('analytics_users') }} u USING (user_id_hex)

      -- ignore empty strings and null strings
WHERE COALESCE(len(continent_code), 0) + COALESCE(len(country_code), 0) + COALESCE(len(state_code), 0) 
        + COALESCE(len(city), 0) + coalesce(len(zip_code), 0) + COALESCE(len(area_code), 0) != 0
        
{% if is_incremental() %}

  AND a.created_at >= (SELECT MAX(updated_at) - INTERVAL '7 days' FROM {{ this }})

{% endif %}

{% if target.name == 'dev' %}

  AND a.created_at >= CURRENT_DATE - INTERVAL '1 week'

{% endif %}