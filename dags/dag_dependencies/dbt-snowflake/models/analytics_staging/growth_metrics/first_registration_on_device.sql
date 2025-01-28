{{
    config(
        tags=['daily'],
        materialized='table',
        unique_key='username',
        snowflake_warehouse='PROD_WH_LARGE'
    )
}}

-- First registration on devices with app installed since 2020-01-01
-- an approximation for new user registration on app, doesnt matter if its 2ndline or TN.
-- eliminate disabled users and make sure its part of a user set to eliminate fraud
WITH first_registration_on_device AS (
    SELECT a.*
    FROM {{ ref('registrations_1') }} a
    JOIN {{ source('adjust','installs') }} b USING (adid)
    WHERE
        (a.created_at >= '2020-01-01')
        AND (b.created_at >= '2020-01-01')
        AND (country_code IS NOT NULL)
        AND (client_version IS NOT NULL)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY adid ORDER BY a.created_at) = 1
)

SELECT a.username, a.created_at, a.client_type, a.client_version, a.country_code, a.adid
FROM first_registration_on_device a
JOIN {{ source('core','users') }} b USING (username)
JOIN {{ source('dau','user_set') }} USING (username)
WHERE (b.account_status = 'ENABLED')
