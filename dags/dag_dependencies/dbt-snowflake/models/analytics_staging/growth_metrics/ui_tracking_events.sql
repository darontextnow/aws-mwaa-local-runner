{{
    config(
        tags=['daily'],
        materialized='incremental',
        unique_key='date_utc',
        snowflake_warehouse='PROD_WH_MEDIUM'
    )
}}

SELECT
    date_utc,
    "client_details.client_data.user_data.username" AS username,
    COALESCE("client_details.ios_bonus_data.adjust_id", "client_details.android_bonus_data.adjust_id") AS adjust_id,
    "client_details.client_data.client_platform" AS platform,
    created_at,
    event_id,
    "client_details.client_data.client_version" AS client_version,
    "payload.properties.UITracking_Category" AS uitracking_category,
    "payload.properties.UITracking_Action" AS uitracking_action,
    "payload.properties.UITracking_Label" AS uitracking_label,
    "payload.properties.UITracking_Value" AS uitracking_value
FROM {{ source('party_planner_realtime', 'property_map') }}
WHERE
    {% if is_incremental() %}
        (date_utc > {{ var('ds') }}::DATE - INTERVAL '3 DAYS')
    {% else %}
        (date_utc > '2024-07-01'::DATE)  -- property_map_parsed table only goes back to this date
    {% endif %}

        AND (COALESCE(
            "payload.properties.UITracking_Category",
            "payload.properties.UITracking_Action",
            "payload.properties.UITracking_Label",
            "payload.properties.UITracking_Value"
        ) IS NOT NULL)
