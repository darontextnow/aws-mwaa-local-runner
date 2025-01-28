{{
    config(
        tags=['daily_features'],
        full_refresh = false,
        materialized='incremental',
        unique_key='report_date',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

{{ generate_user_features_rolling_model(ref('user_features_ui_events')) }}
