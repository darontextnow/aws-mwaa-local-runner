{{
    config(
        tags=['daily'],
        materialized='view'
    )
}}

SELECT
    vendor,
    userid as user_id,
    producttype as product_type,
    packagename as package_name,
    orderid as order_id,
    productid as product_id,
    status,
    createdat as created_at
FROM {{ source('core', 'iap') }}
WHERE
    (producttype = 'IAP_TYPE_CONSUMABLE')
    AND (environment = 'Prod')
    AND (vendor = 'PLAY_STORE')
