{{
    config(
        tags=['daily_features']
    )
}}

{{ generate_user_set_features_rolling_model(ref('user_features_nps')) }}