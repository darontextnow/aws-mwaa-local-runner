{{
    config(
        tags=['daily_trust_safety'],
        materialized='table',
        snowflake_warehouse='PROD_WH_SMALL'
    )
}}

SELECT
    DISTINCT username,
    MAX(CASE WHEN (productid LIKE '%lock%number%'
            OR productid LIKE '%premium%number%'
            OR productid LIKE '%adfree%'
            OR productid LIKE '%premium1monthsubscription%'
            OR productid LIKE '%premium1yearsubscription%'
            OR productid LIKE '%international%credit%'
            OR productid LIKE '%pro_plan%'
            OR productid ILIKE '%vpn%'
            OR productid LIKE '%textnow_pro%')
        THEN 1 ELSE 0 END
    ) purchased
FROM {{ source('core', 'iap') }}
JOIN {{ source('core', 'users') }} ON (user_id_hex = userid)
WHERE
    createdat >= {{ var('current_timestamp') }}::TIMESTAMP_NTZ - INTERVAL '31 days'
    AND status IN (
        'SUBSCRIPTION_PURCHASED',
        'DID_RENEW',
        'INITIAL_BUY',
        'ONE_TIME_PRODUCT_PURCHASED',
        'SUBSCRIPTION_RENEWED'
    )
GROUP BY 1
HAVING purchased = 1
