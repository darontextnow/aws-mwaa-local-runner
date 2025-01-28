{#
    The source table only contains data for the last 30 days, hence we are not going to allow a full refresh.
    This model doesn't have an incremental filter since full refresh isn't allowed for the above reason.
#}

{{
    config(
        tags=['daily_trust_safety'],
        materialized='incremental',
        unique_key='username',
        snowflake_warehouse='PROD_WH_SMALL',
        pre_hook = [
            "--Delete all records older than 60d
            DELETE FROM {{ this }} tgt
            WHERE last_login <= {{ var('current_date') }}::DATE - INTERVAL '60 days'"
        ],
        full_refresh=false
    )
}}

SELECT
    MAX(date_utc) AS last_login,
    username
FROM {{ source('core', 'tn_requests_raw') }}
WHERE
    date_utc >= {{ var('current_date') }}::DATE - INTERVAL '1 days'
    AND route_name IN ('SessionsController_login', 'v3IdentityAuthenticate')
    AND http_response_status = 200
GROUP BY username
