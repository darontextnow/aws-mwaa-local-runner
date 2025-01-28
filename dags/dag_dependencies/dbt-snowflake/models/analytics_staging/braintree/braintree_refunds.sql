{{
    config(
        tags=['daily'],
        materialized='view',
        unique_key='braintree_refund_id'
    )
}}

SELECT
    id AS braintree_refund_id,
    created_at,
    updated_at,
    refunded_transaction_id AS refunded_charge_id,
    'refund' AS category,
    'Braintree' AS payment_provider,
    id AS identifier,  -- braintree transaction id
    amount,
    currency
FROM {{ source('braintree', 'braintree_transactions') }} AS braintree_refunds_transactions
WHERE (status = 'settled') AND ("type" = 'credit') -- this means refund