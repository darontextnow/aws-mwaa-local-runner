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
    FROM {{ ref('user_set_features_active_device_rolling') }}

    {% for t in [
        ref('user_set_features_activities_rolling'),
        ref('user_set_features_age_range_rolling'),
        ref('user_set_features_gender_rolling'),
        ref('user_set_features_interest_rolling'),
        ref('user_set_features_nps_rolling'),
        ref('user_set_features_permissions_rolling'),
        ref('user_set_features_phone_number_rolling'),
        ref('user_set_features_profile_location_rolling'),
        ref('user_set_features_ui_events_rolling'),
        ref('user_set_features_use_cases_rolling'),
        ref('user_set_features_user_segment_rolling')
    ] %}

    LEFT JOIN (
        SELECT * FROM {{ t }} WHERE (report_date = {{ var('ds') }}::DATE)
    ) AS t_{{ loop.index }} USING (report_date, user_set_id)

    {% endfor %}

    WHERE (report_date = {{ var('ds') }}::DATE)
)

SELECT
    cte.*,
    us.first_paid_device_date IS NULL AS is_organic,
    DATEDIFF(DAY, us.created_at, report_date) AS user_set_lifetime_days
FROM cte
LEFT JOIN {{ ref('user_sets') }} us USING (user_set_id)
