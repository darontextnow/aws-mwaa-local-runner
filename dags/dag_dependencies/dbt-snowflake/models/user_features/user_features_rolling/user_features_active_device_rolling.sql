{{
    config(
        tags=['daily_features'],
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

{{ generate_user_features_rolling_model(ref('user_features_active_device')) }}