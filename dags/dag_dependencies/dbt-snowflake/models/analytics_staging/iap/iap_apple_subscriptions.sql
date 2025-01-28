{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}


SELECT
    vendor,
    userid,
    producttype AS product_type,
    transactionid AS transaction_id,
    productid AS product_id,
    status,
    createdat AS started_at,
    CASE
        WHEN LEAD(status) OVER (PARTITION BY userid, productid ORDER BY createdat) = 'CANCEL'
            THEN lead(createdat) OVER (PARTITION BY userid, productid ORDER BY createdat)
        WHEN calculatedexpiredate IS NULL THEN lead(createdat) OVER (PARTITION BY userid, productid ORDER BY createdat)
        ELSE calculatedexpiredate
    END AS expired_at,
    isinbillingretryperiod,
    isinintroofferperiod,
    istrialperiod
FROM {{ source('core', 'iap') }}
WHERE
    (producttype = 'IAP_TYPE_SUBSCRIPTION')
    AND (vendor = 'APP_STORE')
    AND (environment = 'Prod')
    AND (status IN ('INITIAL_BUY', 'DID_RENEW', 'POLL_AUTO_RENEWED', 'DID_RECOVER', 'INTERACTIVE_RENEWAL', 'CANCEL'))
QUALIFY (status != 'CANCEL')  -- use qualify here because it's evaluated after window functions

