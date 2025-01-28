{{
    config(
        tags=['daily_features']
    )
}}

{{ generate_user_features_rolling_model(ref('user_features_permissions')) }}