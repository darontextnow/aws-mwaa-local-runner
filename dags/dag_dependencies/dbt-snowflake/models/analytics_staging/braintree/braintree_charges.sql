{{
    config(
        tags=['daily'],
        materialized='view',
        unique_key='braintree_charge_id'
    )
}}

SELECT
    id AS braintree_charge_id,
    created_at,
    updated_at,
    'capture' AS category,
    'Braintree' AS payment_provider,
    id AS identifier,  -- braintree transaction id
    NVL(amount, 0) AS amount,
    currency
FROM {{ source('braintree', 'braintree_transactions') }} AS braintree_transactions
WHERE (status = 'settled') AND ("type" = 'sale')
