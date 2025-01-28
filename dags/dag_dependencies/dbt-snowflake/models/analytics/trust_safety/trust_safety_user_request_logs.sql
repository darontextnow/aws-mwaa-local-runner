{{
    config(
        tags=['daily_trust_safety'],
        materialized='incremental',
        unique_key='cohort_utc',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

WITH reqs AS (
    SELECT
        request_ts,
        username,
        client_type,
        route_name,
        http_response_status,
        client_ip,
        client_version,
        date_utc,
        user_agent,
        "X-TN-Integrity-Session.attested",
        "X-TN-Integrity-Session.exists",
        "X-TN-Integrity-Session.device_integrity",
        "X-TN-Integrity-Session.app_licensing",
        "px-compromised-credentials",
        "request_params.password",
        authentication_type
    FROM {{ source('core', 'tn_requests_raw') }}
    WHERE

    {% if is_incremental() %}
        (date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 DAY')
    {% else %}
        TRUE  --Note: this table only retains latest 30 days of data.
    {% endif %}

        AND (client_type IN ('TN_ANDROID', 'TN_IOS_FREE', 'TN_WEB', '2L_ANDROID'))
        AND (username IS NOT NULL)
        AND (route_name <> 'SessionsController_getSessionUser')

    UNION ALL SELECT
        request_ts,
        username,
        client_type,
        route_name,
        http_response_status,
        client_ip,
        client_version,
        date_utc,
        user_agent,
        "X-TN-Integrity-Session.attested",
        "X-TN-Integrity-Session.exists",
        "X-TN-Integrity-Session.device_integrity",
        "X-TN-Integrity-Session.app_licensing",
        "px-compromised-credentials",
        "request_params.password",
        authentication_type
    FROM {{ source('core', 'tn_requests2_raw') }}
    WHERE

    {% if is_incremental() %}
        (date_utc >= {{ var('ds') }}::DATE - INTERVAL '1 DAY')
    {% else %}
        TRUE --Note: this table only retains latest 30 days of data.
    {% endif %}

        AND (client_type IN ('TN_ANDROID', 'TN_IOS_FREE', 'TN_WEB', '2L_ANDROID'))
        AND (username IS NOT NULL)
)

SELECT *
FROM reqs
MATCH_RECOGNIZE (
    PARTITION BY username ORDER BY request_ts
    MEASURES MATCH_SEQUENCE_NUMBER() AS match_sequence_number,
    FINAL FIRST(reg_login.request_ts)::DATE AS cohort_utc
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (reg_login notreg_login{,30})
    DEFINE
        REG_LOGIN AS
            route_name IN ('UsersController_register', 'SessionsController_login', 'v3IdentityAuthenticate')
            AND (http_response_status = 200),
        NOTREG_LOGIN AS route_name NOT IN ( 'UsersController_register', 'SessionsController_login', 'v3IdentityAuthenticate')
)
