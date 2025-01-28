{{
    config(
        tags=['daily_features'],
        full_refresh=false,
        materialized='incremental',
        unique_key='report_date',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH cte AS (
    SELECT *
    FROM {{ ref('user_features_active_device_rolling') }}

    {% for t in [
        ref('user_features_activities_rolling'),
        ref('user_features_age_range_rolling'),
        ref('user_features_gender_rolling'),
        ref('user_features_interest_rolling'),
        ref('user_features_nps_rolling'),
        ref('user_features_permissions_rolling'),
        ref('user_features_phone_number_rolling'),
        ref('user_features_profile_location_rolling'),
        ref('user_features_ui_events_rolling'),
        ref('user_features_use_cases_rolling'),
        ref('user_features_user_segment_rolling')
    ] %}

    LEFT JOIN (
        SELECT * FROM {{ t }} WHERE (report_date = {{ var('ds') }}::DATE)
    ) AS t_{{ loop.index }} USING (report_date, username)

    {% endfor %}

    WHERE (report_date = {{ var('ds') }}::DATE)
)

SELECT
    cte.*,
    is_organic,
    DATEDIFF(DAY, user_created_at::DATE, report_date::DATE) AS user_lifetime_days,
    DATEDIFF(DAY, user_set_created_at::DATE, report_date::DATE) AS user_set_lifetime_days
FROM cte
LEFT JOIN {{ ref('user_features_user_acquisition') }} USING (username)
