{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='surrogate_key'
    )
}}

WITH attributed_30d AS (

    SELECT
        surrogate_key,
        client_type,
        adjust_id,
        installed_at,
        impression_datetime                      AS impressed_at,
        'Tatari Streaming TV (app)'::varchar(25) AS network_name,
        platform                                 AS campaign_name,
        campaign_id                              AS adgroup_name,
        creative_code                            AS creative_name,
        'tatari_viewthrough'                     AS match_type,
        device_type,
        row_number() OVER (PARTITION BY adjust_id ORDER BY impression_datetime DESC) row_number
    FROM {{ source('ua', 'tatari_user_attribution_view_through_streaming_app') }}
    JOIN {{ ref('adjust_installs_stage') }} ON adjust_id = device_id
    WHERE event_type = 'install'
      AND is_organic
      AND installed_at BETWEEN impression_datetime AND impression_datetime + interval '30 days'

)

SELECT
    surrogate_key,
    client_type,
    adjust_id,
    installed_at,
    impressed_at,
    't_'|| {{ dbt_utils.generate_surrogate_key(['network_name', 'campaign_name', 'adgroup_name', 'creative_name']) }} AS tracker,
    false AS is_organic,
    network_name,
    campaign_name,
    adgroup_name,
    creative_name,
    match_type,
    device_type
FROM attributed_30d
WHERE row_number = 1
